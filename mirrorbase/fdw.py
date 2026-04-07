"""Foreign Data Wrapper layer for instant database availability.

Instead of copying all data before making a clone available, we:
1. Set up FDW pointing to source — instant, zero data transfer
2. Create local overlay tables for writes (INSERT/UPDATE/DELETE)
3. Create views that merge local writes with remote reads
4. Background worker copies tables locally for performance
5. As tables finish copying, swap from FDW to local seamlessly
"""

import subprocess
import threading
import time
from typing import Optional, Callable
from urllib.parse import urlparse, parse_qs

import psycopg2
import psycopg2.extras

from .config import PG_BIN
from .postgres import LocalPostgres
from .exceptions import ReplicationError


def setup_fdw(
    local_pg: LocalPostgres,
    source_connstring: str,
    source_dbname: str,
    local_dbname: str,
):
    """Set up FDW + local overlay tables for instant availability.

    For each source table T:
    1. Foreign table: _fdw_T (reads from source via FDW)
    2. Local writes table: _local_T (stores inserts/updates)
    3. Local deletes table: _tombstone_T (tracks deleted PKs)
    4. View: T (merges local + remote, hides tombstoned rows)
    5. INSTEAD OF triggers on view for INSERT/UPDATE/DELETE
    """
    parsed = urlparse(source_connstring)
    host = parsed.hostname
    port = parsed.port or 5432
    user = parsed.username
    password = parsed.password
    dbname = parsed.path.lstrip("/")

    # Parse SSL options
    query_params = parse_qs(parsed.query)
    sslmode = query_params.get("sslmode", ["prefer"])[0]

    conn = psycopg2.connect(
        host="localhost", port=local_pg.port,
        user="mirrorbase", dbname=local_dbname,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Install postgres_fdw extension
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")

        # Create foreign server
        cur.execute(f"""
            CREATE SERVER IF NOT EXISTS mirrorbase_source
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (
                host '{host}',
                port '{port}',
                dbname '{dbname}',
                sslmode '{sslmode}'
            )
        """)

        # Create user mapping
        cur.execute(f"""
            CREATE USER MAPPING IF NOT EXISTS FOR mirrorbase
            SERVER mirrorbase_source
            OPTIONS (user '{user}', password '{password}')
        """)

        # Import all foreign tables into a separate schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS _fdw")
        cur.execute("""
            IMPORT FOREIGN SCHEMA public
            FROM SERVER mirrorbase_source
            INTO _fdw
        """)

        # Get list of imported foreign tables
        cur.execute("""
            SELECT foreign_table_name
            FROM information_schema.foreign_tables
            WHERE foreign_table_schema = '_fdw'
        """)
        tables = [row[0] for row in cur.fetchall()]

        # Create overlay schema for local writes
        cur.execute("CREATE SCHEMA IF NOT EXISTS _local")
        cur.execute("CREATE SCHEMA IF NOT EXISTS _tombstone")

        for table in tables:
            _create_overlay_for_table(cur, table)

    conn.close()
    return tables


def _get_table_columns(cur, schema: str, table: str) -> list[tuple[str, str]]:
    """Get column names and types for a table."""
    cur.execute("""
        SELECT column_name, data_type, udt_name,
               is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return cur.fetchall()


def _get_primary_key_columns(cur, source_connstring: str, table: str) -> list[str]:
    """Get PK columns from the source database."""
    source_conn = psycopg2.connect(source_connstring)
    source_conn.autocommit = True
    with source_conn.cursor() as src_cur:
        src_cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary AND n.nspname = 'public' AND c.relname = %s
        """, (table,))
        pk_cols = [r[0] for r in src_cur.fetchall()]
    source_conn.close()
    return pk_cols


def _create_overlay_for_table(cur, table: str):
    """Create the local overlay + view + triggers for one table."""
    # Get columns from FDW table
    cur.execute(f"""
        SELECT
            a.attname,
            pg_catalog.format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '_fdw' AND c.relname = %s
        AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """, (table,))
    columns = cur.fetchall()

    if not columns:
        return

    col_names = [c[0] for c in columns]
    col_defs = ", ".join(f'"{c[0]}" {c[1]}' for c in columns)
    col_list = ", ".join(f'"{c}"' for c in col_names)

    # Try to find PK columns from the FDW table's source
    # We'll check if there's an id column or similar
    pk_cols = []
    for name, _ in columns:
        if name == "id" or name.endswith("_id"):
            pk_cols = [name]
            break
    if not pk_cols and columns:
        pk_cols = [columns[0][0]]  # fallback to first column

    pk_list = ", ".join(f'"{c}"' for c in pk_cols)
    pk_where = " AND ".join(f'_local."{c}" = NEW."{c}"' for c in pk_cols)
    pk_where_old = " AND ".join(f'_tombstone."{c}" = OLD."{c}"' for c in pk_cols)
    pk_where_tomb = " AND ".join(f'_tombstone_tbl."{c}" = _fdw."{c}"' for c in pk_cols)

    # 1. Local writes table (same schema as source, with PK)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS _local."{table}" ({col_defs}, PRIMARY KEY ({pk_list}))
    """)

    # 2. Tombstone table (tracks deletes by PK)
    tomb_defs = ", ".join(f'"{c[0]}" {c[1]}' for c in columns if c[0] in pk_cols)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS _tombstone."{table}" ({tomb_defs}, PRIMARY KEY ({pk_list}))
    """)

    # 3. Drop existing public table/view if it exists (from FDW import or prior run)
    cur.execute(f'DROP VIEW IF EXISTS public."{table}" CASCADE')
    cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')

    # 4. Create the merged view
    # Logic: local writes UNION ALL (remote rows NOT IN tombstones AND NOT IN local overrides)
    pk_not_in_local = " AND ".join(
        f'_fdw."{c}" NOT IN (SELECT "{c}" FROM _local."{table}")'
        for c in pk_cols
    )
    pk_not_in_tomb = " AND ".join(
        f'_fdw."{c}" NOT IN (SELECT "{c}" FROM _tombstone."{table}")'
        for c in pk_cols
    )

    cur.execute(f"""
        CREATE VIEW public."{table}" AS
        SELECT {col_list} FROM _local."{table}"
        UNION ALL
        SELECT {col_list} FROM _fdw."{table}" _fdw
        WHERE {pk_not_in_local}
        AND {pk_not_in_tomb}
    """)

    # 5. INSTEAD OF triggers for writes
    new_values = ", ".join(f"NEW.\"{c}\"" for c in col_names)

    cur.execute(f"""
        CREATE OR REPLACE FUNCTION _local."{table}_insert_fn"()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO _local."{table}" ({col_list}) VALUES ({new_values});
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    cur.execute(f"""
        CREATE TRIGGER "{table}_insert_trigger"
        INSTEAD OF INSERT ON public."{table}"
        FOR EACH ROW EXECUTE FUNCTION _local."{table}_insert_fn"()
    """)

    # DELETE trigger — add to tombstone
    old_pk_values = ", ".join(f"OLD.\"{c}\"" for c in pk_cols)
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION _local."{table}_delete_fn"()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO _tombstone."{table}" ({pk_list}) VALUES ({old_pk_values})
            ON CONFLICT DO NOTHING;
            DELETE FROM _local."{table}" WHERE {" AND ".join(f'"{c}" = OLD."{c}"' for c in pk_cols)};
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql
    """)
    cur.execute(f"""
        CREATE TRIGGER "{table}_delete_trigger"
        INSTEAD OF DELETE ON public."{table}"
        FOR EACH ROW EXECUTE FUNCTION _local."{table}_delete_fn"()
    """)

    # UPDATE trigger — upsert into local
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION _local."{table}_update_fn"()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM _local."{table}" WHERE {" AND ".join(f'"{c}" = OLD."{c}"' for c in pk_cols)};
            INSERT INTO _local."{table}" ({col_list}) VALUES ({new_values});
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    cur.execute(f"""
        CREATE TRIGGER "{table}_update_trigger"
        INSTEAD OF UPDATE ON public."{table}"
        FOR EACH ROW EXECUTE FUNCTION _local."{table}_update_fn"()
    """)


class BackgroundMigrator:
    """Copies tables from source to local, then sets up real-time replication.

    Phase 1: Migrate tables from FDW to local via parallel COPY
    Phase 2: Promote local tables to public schema (drop views)
    Phase 3: CREATE SUBSCRIPTION for real-time push-based sync
    """

    def __init__(
        self,
        source_connstring: str,
        local_pg: LocalPostgres,
        local_dbname: str,
        tables: list[str],
        logical_available: bool = False,
        on_progress: Optional[Callable] = None,
    ):
        self.source_connstring = source_connstring
        self.local_pg = local_pg
        self.local_dbname = local_dbname
        self.tables = tables
        self.logical_available = logical_available
        self.on_progress = on_progress
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.migrated: set[str] = set()
        self.replication_active: bool = False

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        # Phase 1: Migrate all tables from FDW to local
        for i, table in enumerate(self.tables):
            if self._stop.is_set():
                return
            try:
                self._migrate_table(table)
                self.migrated.add(table)
                if self.on_progress:
                    self.on_progress(table, "migrated", len(self.migrated), len(self.tables))
            except Exception as e:
                if self.on_progress:
                    self.on_progress(table, f"error: {e}", len(self.migrated), len(self.tables))

        # Phase 2: Promote tables + start real-time replication
        if self.logical_available:
            try:
                from .streaming import promote_to_real_tables, setup_realtime_replication
                import uuid

                pub_name = f"mirrorbase_pub_{uuid.uuid4().hex[:8]}"
                sub_name = f"mirrorbase_sub_{uuid.uuid4().hex[:8]}"

                promoted = promote_to_real_tables(self.local_pg, self.local_dbname)
                if self.on_progress:
                    self.on_progress("schema", f"promoted {len(promoted)} tables", 0, 0)

                setup_realtime_replication(
                    self.local_pg, self.source_connstring,
                    self.local_dbname, pub_name, sub_name,
                )
                self.replication_active = True
                if self.on_progress:
                    self.on_progress("replication", "real-time streaming active", 0, 0)
            except Exception as e:
                if self.on_progress:
                    self.on_progress("replication", f"error: {e}", 0, 0)
        else:
            if self.on_progress:
                self.on_progress("sync", "logical replication not available — enable wal_level=logical for real-time sync", 0, 0)

    def _migrate_table(self, table: str):
        """Copy a table from source to local, then swap the view to use local data."""
        source_conn = psycopg2.connect(self.source_connstring)
        source_conn.autocommit = True
        local_conn = psycopg2.connect(
            host="localhost", port=self.local_pg.port,
            user="mirrorbase", dbname=self.local_dbname,
        )
        local_conn.autocommit = True

        with source_conn.cursor() as src_cur, local_conn.cursor() as local_cur:
            # Get columns
            local_cur.execute(f"""
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = '_fdw' AND c.relname = %s
                AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (table,))
            columns = local_cur.fetchall()
            col_names = [c[0] for c in columns]
            col_list = ", ".join(f'"{c}"' for c in col_names)

            # Copy all data from source
            src_cur.execute(f'SELECT {col_list} FROM public."{table}"')
            rows = src_cur.fetchall()

            if rows:
                placeholders = ", ".join(["%s"] * len(col_names))
                psycopg2.extras.execute_batch(
                    local_cur,
                    f'INSERT INTO _local."{table}" ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                    rows,
                    page_size=1000,
                )

            # Now update the view to only use local data (no more FDW)
            local_cur.execute(f'DROP VIEW IF EXISTS public."{table}" CASCADE')
            local_cur.execute(f"""
                CREATE VIEW public."{table}" AS
                SELECT {col_list} FROM _local."{table}"
            """)

            # Recreate triggers on the new view
            new_values = ", ".join(f'NEW."{c}"' for c in col_names)
            pk_cols = [col_names[0]]  # simplified
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            old_pk_values = ", ".join(f'OLD."{c}"' for c in pk_cols)

            local_cur.execute(f"""
                CREATE TRIGGER "{table}_insert_trigger"
                INSTEAD OF INSERT ON public."{table}"
                FOR EACH ROW EXECUTE FUNCTION _local."{table}_insert_fn"()
            """)
            local_cur.execute(f"""
                CREATE TRIGGER "{table}_delete_trigger"
                INSTEAD OF DELETE ON public."{table}"
                FOR EACH ROW EXECUTE FUNCTION _local."{table}_delete_fn"()
            """)
            local_cur.execute(f"""
                CREATE TRIGGER "{table}_update_trigger"
                INSTEAD OF UPDATE ON public."{table}"
                FOR EACH ROW EXECUTE FUNCTION _local."{table}_update_fn"()
            """)

        source_conn.close()
        local_conn.close()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
