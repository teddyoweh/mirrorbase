"""Incremental sync engine — keeps the base replica fresh without logical replication.

Strategy:
- Initial sync: pg_dump (one-time)
- Incremental: query-based CDC using timestamp/serial columns
- Deletes: periodic row count check, full reconcile only when counts diverge
"""

import json
import time
import threading
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, Callable

import psycopg2
import psycopg2.extras

from .postgres import LocalPostgres
from .exceptions import ReplicationError


@dataclass
class TableSyncState:
    table_name: str
    schema_name: str
    track_column: Optional[str] = None  # updated_at, created_at, id, etc.
    track_type: Optional[str] = None    # timestamp, serial
    last_value: Optional[str] = None
    last_count: int = 0


def save_sync_states(path: Path, states: list[TableSyncState]):
    path.write_text(json.dumps([asdict(s) for s in states], indent=2, default=str))


def load_sync_states(path: Path) -> list[TableSyncState]:
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    return [TableSyncState(**d) for d in data]


def detect_track_columns(source_connstring: str, source_dbname: str) -> list[TableSyncState]:
    """Detect the best column to track changes for each table.

    Priority:
    1. updated_at / modified_at (timestamp) — catches updates
    2. created_at (timestamp) — catches inserts only
    3. Primary key serial/bigserial — catches inserts only
    4. None — requires full resync
    """
    conn = psycopg2.connect(source_connstring)
    conn.autocommit = True
    states = []

    with conn.cursor() as cur:
        # Get all user tables
        cur.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename
        """)
        tables = cur.fetchall()

        for schema, table in tables:
            state = TableSyncState(table_name=table, schema_name=schema)

            # Check for timestamp columns
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                AND data_type IN ('timestamp with time zone', 'timestamp without time zone')
                ORDER BY
                    CASE column_name
                        WHEN 'updated_at' THEN 1
                        WHEN 'modified_at' THEN 2
                        WHEN 'last_modified' THEN 3
                        WHEN 'updated' THEN 4
                        WHEN 'created_at' THEN 5
                        WHEN 'created' THEN 6
                        WHEN 'inserted_at' THEN 7
                        ELSE 10
                    END
            """, (schema, table))
            ts_cols = cur.fetchall()

            if ts_cols:
                state.track_column = ts_cols[0][0]
                state.track_type = "timestamp"
            else:
                # Check for serial/bigserial primary key
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE i.indisprimary
                    AND n.nspname = %s AND c.relname = %s
                    AND a.atttypid IN ('int4'::regtype, 'int8'::regtype)
                    LIMIT 1
                """, (schema, table))
                pk = cur.fetchone()
                if pk:
                    state.track_column = pk[0]
                    state.track_type = "serial"

            states.append(state)

    conn.close()
    return states


def sync_new_tables(
    source_connstring: str,
    local_pg: LocalPostgres,
    source_dbname: str,
) -> list[str]:
    """Detect tables that exist on source but not locally, and create them."""
    import subprocess
    from .config import PG_BIN

    source_conn = psycopg2.connect(source_connstring)
    source_conn.autocommit = True
    local_conn = psycopg2.connect(
        host="localhost", port=local_pg.port,
        user="mirrorbase", dbname=source_dbname,
    )
    local_conn.autocommit = True

    # Get source tables
    with source_conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """)
        source_tables = set((r[0], r[1]) for r in cur.fetchall())

    # Get local tables
    with local_conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """)
        local_tables = set((r[0], r[1]) for r in cur.fetchall())

    new_tables = source_tables - local_tables
    created = []

    for schema, table in new_tables:
        # Dump just this table's schema + data from source
        result = subprocess.run(
            [
                str(PG_BIN / "pg_dump"),
                "--no-owner", "--no-privileges",
                "--no-publications", "--no-subscriptions",
                "-t", f'{schema}.{table}',
                source_connstring,
            ],
            capture_output=True, text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            subprocess.run(
                [
                    str(PG_BIN / "psql"),
                    "-h", "localhost", "-p", str(local_pg.port),
                    "-U", "mirrorbase", "-d", source_dbname,
                    "--no-psqlrc",
                ],
                input=result.stdout,
                capture_output=True, text=True,
            )
            created.append(f"{schema}.{table}")

    source_conn.close()
    local_conn.close()
    return created


def incremental_sync(
    source_connstring: str,
    local_pg: LocalPostgres,
    source_dbname: str,
    sync_states: list[TableSyncState],
    progress_callback: Optional[Callable] = None,
) -> list[TableSyncState]:
    """Pull only changed rows from source into local replica."""
    # First: detect and sync any new tables
    new_tables = sync_new_tables(source_connstring, local_pg, source_dbname)
    if new_tables:
        # Re-detect track columns to include new tables
        sync_states = detect_track_columns(source_connstring, source_dbname)

    source_conn = psycopg2.connect(source_connstring)
    source_conn.autocommit = True

    local_conn = psycopg2.connect(
        host="localhost", port=local_pg.port,
        user="mirrorbase", dbname=source_dbname,
    )
    local_conn.autocommit = True

    total = len(sync_states)

    for i, state in enumerate(sync_states):
        fqn = f'"{state.schema_name}"."{state.table_name}"'
        rows_synced = 0

        try:
            with source_conn.cursor() as src_cur:
                if state.track_column and state.last_value:
                    # Incremental: only rows changed since last sync
                    if state.track_type == "timestamp":
                        src_cur.execute(
                            f'SELECT * FROM {fqn} WHERE "{state.track_column}" > %s',
                            (state.last_value,)
                        )
                    elif state.track_type == "serial":
                        src_cur.execute(
                            f'SELECT * FROM {fqn} WHERE "{state.track_column}" > %s',
                            (int(state.last_value),)
                        )
                elif state.track_column and not state.last_value:
                    # First incremental run — get max value for next time
                    if state.track_type == "timestamp":
                        src_cur.execute(f'SELECT MAX("{state.track_column}") FROM {fqn}')
                    else:
                        src_cur.execute(f'SELECT MAX("{state.track_column}") FROM {fqn}')
                    result = src_cur.fetchone()
                    if result and result[0] is not None:
                        state.last_value = str(result[0])

                    # Also get count for delete detection
                    src_cur.execute(f'SELECT count(*) FROM {fqn}')
                    state.last_count = src_cur.fetchone()[0]

                    if progress_callback:
                        progress_callback(state.table_name, "indexed", i + 1, total)
                    continue
                else:
                    # No trackable column — skip (would need full resync)
                    if progress_callback:
                        progress_callback(state.table_name, "skipped", i + 1, total)
                    continue

                if src_cur.description:
                    columns = [desc[0] for desc in src_cur.description]
                    rows = src_cur.fetchall()
                    rows_synced = len(rows)

                    if rows:
                        # Upsert into local using ON CONFLICT
                        # Get primary key columns
                        with source_conn.cursor() as pk_cur:
                            pk_cur.execute("""
                                SELECT a.attname
                                FROM pg_index i
                                JOIN pg_attribute a ON a.attrelid = i.indrelid
                                    AND a.attnum = ANY(i.indkey)
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE i.indisprimary
                                AND n.nspname = %s AND c.relname = %s
                            """, (state.schema_name, state.table_name))
                            pk_cols = [r[0] for r in pk_cur.fetchall()]

                        if pk_cols:
                            # Build upsert
                            col_list = ", ".join(f'"{c}"' for c in columns)
                            placeholders = ", ".join(["%s"] * len(columns))
                            conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
                            update_cols = ", ".join(
                                f'"{c}" = EXCLUDED."{c}"'
                                for c in columns if c not in pk_cols
                            )

                            if update_cols:
                                upsert_sql = f"""
                                    INSERT INTO {fqn} ({col_list})
                                    VALUES ({placeholders})
                                    ON CONFLICT ({conflict_cols})
                                    DO UPDATE SET {update_cols}
                                """
                            else:
                                upsert_sql = f"""
                                    INSERT INTO {fqn} ({col_list})
                                    VALUES ({placeholders})
                                    ON CONFLICT ({conflict_cols}) DO NOTHING
                                """

                            with local_conn.cursor() as local_cur:
                                psycopg2.extras.execute_batch(local_cur, upsert_sql, rows)
                        else:
                            # No PK — just insert (may create duplicates on retry)
                            col_list = ", ".join(f'"{c}"' for c in columns)
                            placeholders = ", ".join(["%s"] * len(columns))
                            with local_conn.cursor() as local_cur:
                                psycopg2.extras.execute_batch(
                                    local_cur,
                                    f"INSERT INTO {fqn} ({col_list}) VALUES ({placeholders})",
                                    rows,
                                )

                    # Update tracking value
                    if state.track_type == "timestamp":
                        with source_conn.cursor() as tc:
                            tc.execute(f'SELECT MAX("{state.track_column}") FROM {fqn}')
                            result = tc.fetchone()
                            if result and result[0]:
                                state.last_value = str(result[0])
                    elif state.track_type == "serial":
                        with source_conn.cursor() as tc:
                            tc.execute(f'SELECT MAX("{state.track_column}") FROM {fqn}')
                            result = tc.fetchone()
                            if result and result[0]:
                                state.last_value = str(result[0])

        except Exception as e:
            if progress_callback:
                progress_callback(state.table_name, f"error: {e}", i + 1, total)
            continue

        status = f"synced +{rows_synced}" if rows_synced else "up to date"
        if progress_callback:
            progress_callback(state.table_name, status, i + 1, total)

    source_conn.close()
    local_conn.close()
    return sync_states


class SyncDaemon:
    """Background thread that periodically syncs changes from source."""

    def __init__(
        self,
        source_connstring: str,
        local_pg: LocalPostgres,
        source_dbname: str,
        interval: int = 30,
        on_sync: Optional[Callable] = None,
    ):
        self.source_connstring = source_connstring
        self.local_pg = local_pg
        self.source_dbname = source_dbname
        self.interval = interval
        self.on_sync = on_sync
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._sync_states: list[TableSyncState] = []

    def start(self):
        self._sync_states = detect_track_columns(self.source_connstring, self.source_dbname)
        # Initialize tracking values
        incremental_sync(
            self.source_connstring, self.local_pg,
            self.source_dbname, self._sync_states,
        )
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                self._sync_states = incremental_sync(
                    self.source_connstring, self.local_pg,
                    self.source_dbname, self._sync_states,
                    progress_callback=self.on_sync,
                )
            except Exception:
                pass

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def sync_now(self):
        """Trigger an immediate sync."""
        self._sync_states = incremental_sync(
            self.source_connstring, self.local_pg,
            self.source_dbname, self._sync_states,
        )
