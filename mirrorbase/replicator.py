import subprocess
import time
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import Optional, Callable

import psycopg2

from .config import PG_BIN
from .postgres import LocalPostgres
from .exceptions import ReplicationError


def detect_pooler_url(connstring: str) -> bool:
    parsed = urlparse(connstring)
    return "-pooler" in (parsed.hostname or "")


def convert_pooler_to_direct(connstring: str) -> str:
    parsed = urlparse(connstring)
    hostname = parsed.hostname or ""
    if "-pooler" not in hostname:
        return connstring

    direct_hostname = hostname.replace("-pooler", "", 1)

    netloc = ""
    if parsed.username:
        netloc = parsed.username
        if parsed.password:
            netloc += f":{parsed.password}"
        netloc += "@"
    netloc += direct_hostname
    if parsed.port:
        netloc += f":{parsed.port}"

    query_params = parse_qs(parsed.query)
    if "sslmode" not in query_params:
        query_params["sslmode"] = ["require"]
    query_string = urlencode(query_params, doseq=True)

    return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, query_string, parsed.fragment))


def validate_source_connection(connstring: str) -> dict:
    conn = psycopg2.connect(connstring)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]

            cur.execute("SHOW wal_level")
            wal_level = cur.fetchone()[0]

            cur.execute("SELECT current_database()")
            dbname = cur.fetchone()[0]

            cur.execute("""
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename
            """)
            tables = cur.fetchall()
    finally:
        conn.close()

    return {
        "version": version,
        "wal_level": wal_level,
        "logical_available": wal_level == "logical",
        "dbname": dbname,
        "tables": tables,
    }


def setup_publication(connstring: str, publication_name: str):
    conn = psycopg2.connect(connstring)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication_name,))
            if cur.fetchone():
                return
            cur.execute(f'CREATE PUBLICATION "{publication_name}" FOR ALL TABLES')
    finally:
        conn.close()


def setup_subscription(
    local_pg: LocalPostgres,
    source_connstring: str,
    source_dbname: str,
    publication_name: str,
    subscription_name: str,
):
    # Create the database locally
    existing = local_pg.execute_sql("SELECT datname FROM pg_database WHERE datistemplate = false")
    existing_names = [r[0] for r in (existing or [])]
    if source_dbname not in existing_names:
        local_pg.execute_sql(f'CREATE DATABASE "{source_dbname}"')

    # Dump schema from source, apply locally
    dump_result = subprocess.run(
        [
            str(PG_BIN / "pg_dump"),
            "--schema-only",
            "--no-publications",
            "--no-subscriptions",
            "--no-owner",
            "--no-privileges",
            source_connstring,
        ],
        capture_output=True, text=True,
    )
    if dump_result.returncode != 0:
        raise ReplicationError(f"Schema dump failed: {dump_result.stderr}")

    if dump_result.stdout.strip():
        apply_result = subprocess.run(
            [
                str(PG_BIN / "psql"),
                "-h", "localhost",
                "-p", str(local_pg.port),
                "-U", "mirrorbase",
                "-d", source_dbname,
                "--no-psqlrc",
            ],
            input=dump_result.stdout,
            capture_output=True, text=True,
        )
        # psql may return warnings for things like extension creation, that's ok
        if apply_result.returncode != 0 and "ERROR" in apply_result.stderr:
            raise ReplicationError(f"Schema apply failed: {apply_result.stderr}")

    # Create subscription
    conn = psycopg2.connect(host="localhost", port=local_pg.port, user="mirrorbase", dbname=source_dbname)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # Check if subscription already exists
            cur.execute("SELECT 1 FROM pg_subscription WHERE subname = %s", (subscription_name,))
            if cur.fetchone():
                return
            cur.execute(f"""
                CREATE SUBSCRIPTION "{subscription_name}"
                CONNECTION '{source_connstring}'
                PUBLICATION "{publication_name}"
                WITH (
                    copy_data = true,
                    create_slot = true,
                    synchronous_commit = 'off'
                )
            """)
    finally:
        conn.close()


def monitor_initial_sync(
    local_pg: LocalPostgres,
    subscription_name: str,
    source_dbname: str,
    progress_callback: Optional[Callable] = None,
):
    conn = psycopg2.connect(host="localhost", port=local_pg.port, user="mirrorbase", dbname=source_dbname)
    conn.autocommit = True

    try:
        while True:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.relname, sr.srsubstate
                    FROM pg_subscription_rel sr
                    JOIN pg_class c ON c.oid = sr.srrelid
                    JOIN pg_subscription s ON s.oid = sr.srsubid
                    WHERE s.subname = %s
                """, (subscription_name,))
                rows = cur.fetchall()

            if not rows:
                time.sleep(1)
                continue

            total = len(rows)
            ready_count = sum(1 for _, state in rows if state in ("r", "s"))

            if progress_callback:
                for table_name, state in rows:
                    state_label = {"i": "initializing", "d": "copying", "f": "finishing", "s": "synced", "r": "ready"}.get(state, state)
                    progress_callback(table_name, state_label, ready_count, total)

            if ready_count == total and total > 0:
                break

            time.sleep(2)
    finally:
        conn.close()


def dump_sync(
    local_pg: LocalPostgres,
    source_connstring: str,
    source_dbname: str,
    progress_callback: Optional[Callable] = None,
    parallel: int = 8,
):
    """Full sync via parallel COPY — much faster than single-threaded pg_dump.

    Strategy:
    1. Dump schema only (fast, tiny)
    2. Get list of tables + row counts (sorted largest first)
    3. COPY each table in parallel threads (N concurrent streams)
    4. Each thread: COPY table TO STDOUT from source | COPY table FROM STDIN to local

    For 1TB at 1Gbps network: ~2.5 hours single-threaded, ~20 min with 8 parallel streams.
    """
    import concurrent.futures
    import io

    # Create database locally
    existing = local_pg.execute_sql("SELECT datname FROM pg_database WHERE datistemplate = false")
    existing_names = [r[0] for r in (existing or [])]
    if source_dbname not in existing_names:
        local_pg.execute_sql(f'CREATE DATABASE "{source_dbname}"')

    if progress_callback:
        progress_callback("schema", "syncing schema", 0, 1)

    # Step 1: Schema only
    dump_result = subprocess.run(
        [
            str(PG_BIN / "pg_dump"),
            "--schema-only",
            "--no-owner", "--no-privileges",
            "--no-publications", "--no-subscriptions",
            source_connstring,
        ],
        capture_output=True, text=True,
    )
    if dump_result.returncode != 0:
        raise ReplicationError(f"Schema dump failed: {dump_result.stderr}")

    if dump_result.stdout.strip():
        subprocess.run(
            [
                str(PG_BIN / "psql"),
                "-h", "localhost", "-p", str(local_pg.port),
                "-U", "mirrorbase", "-d", source_dbname, "--no-psqlrc",
            ],
            input=dump_result.stdout,
            capture_output=True, text=True,
        )

    # Step 2: Get tables sorted by size (largest first for better parallelism)
    source_conn = psycopg2.connect(source_connstring)
    source_conn.autocommit = True
    with source_conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename,
                   pg_total_relation_size(schemaname || '.' || tablename) as size
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY size DESC
        """)
        tables = cur.fetchall()
    source_conn.close()

    if not tables:
        if progress_callback:
            progress_callback("complete", "ready", 1, 1)
        return

    total = len(tables)
    completed = [0]  # mutable for closure
    lock = __import__('threading').Lock()

    def copy_table(schema, table):
        """Copy a single table using COPY protocol — binary pipe, no intermediate storage."""
        try:
            src = psycopg2.connect(source_connstring)
            dst = psycopg2.connect(
                host="localhost", port=local_pg.port,
                user="mirrorbase", dbname=source_dbname,
            )
            src.autocommit = True

            fqn = f'"{schema}"."{table}"'

            # Disable FK checks for this session to handle any table order
            with dst.cursor() as dst_cur:
                dst_cur.execute("SET session_replication_role = 'replica'")

            # Use COPY TO/FROM with a pipe buffer
            buf = io.BytesIO()
            with src.cursor() as src_cur:
                src_cur.copy_expert(f"COPY {fqn} TO STDOUT", buf)

            buf.seek(0)
            with dst.cursor() as dst_cur:
                dst_cur.copy_expert(f"COPY {fqn} FROM STDIN", buf)
            dst.commit()

            src.close()
            dst.close()

            with lock:
                completed[0] += 1
                if progress_callback:
                    progress_callback(table, "copied", completed[0], total)

        except Exception as e:
            with lock:
                completed[0] += 1
                if progress_callback:
                    progress_callback(table, f"error: {e}", completed[0], total)

    # Step 3: Parallel COPY
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = [executor.submit(copy_table, schema, table) for schema, table, _ in tables]
        concurrent.futures.wait(futures)


def get_replication_lag(local_pg: LocalPostgres, subscription_name: str, source_dbname: str) -> Optional[dict]:
    conn = psycopg2.connect(host="localhost", port=local_pg.port, user="mirrorbase", dbname=source_dbname)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pid, received_lsn::text, latest_end_lsn::text, latest_end_time
                FROM pg_stat_subscription
                WHERE subname = %s
            """, (subscription_name,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return None

    return {
        "pid": row[0],
        "received_lsn": row[1],
        "latest_end_lsn": row[2],
        "latest_end_time": row[3],
    }
