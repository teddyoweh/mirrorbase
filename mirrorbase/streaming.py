"""Production-grade real-time sync via logical replication.

After background migration completes:
1. Promote _local tables to public schema (real tables, not views)
2. CREATE SUBSCRIPTION for push-based real-time sync
3. Source pushes INSERT/UPDATE/DELETE via WAL stream
4. Millisecond latency, zero polling
"""

import psycopg2
import psycopg2.extras

from .postgres import LocalPostgres
from .exceptions import ReplicationError


def promote_to_real_tables(local_pg: LocalPostgres, dbname: str) -> list[str]:
    """Convert FDW views + overlay tables into real public tables.

    Steps for each table:
    1. DROP the public view (was merging FDW + local overlay)
    2. ALTER TABLE _local."table" SET SCHEMA public (promote to real table)
    3. Drop FDW infrastructure (_fdw schema, _tombstone schema)
    """
    conn = psycopg2.connect(
        host="localhost", port=local_pg.port,
        user="mirrorbase", dbname=dbname,
    )
    conn.autocommit = True

    promoted = []
    with conn.cursor() as cur:
        # Get all tables in _local schema
        cur.execute("""
            SELECT tablename FROM pg_tables WHERE schemaname = '_local'
            ORDER BY tablename
        """)
        tables = [r[0] for r in cur.fetchall()]

        for table in tables:
            # Drop the public view
            cur.execute(f'DROP VIEW IF EXISTS public."{table}" CASCADE')

            # Drop any public table that might conflict
            cur.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')

            # Promote _local table to public schema
            cur.execute(f'ALTER TABLE _local."{table}" SET SCHEMA public')

            # Drop trigger functions (no longer needed — replication handles sync)
            for fn_suffix in ['_insert_fn', '_delete_fn', '_update_fn']:
                cur.execute(f'DROP FUNCTION IF EXISTS _local."{table}{fn_suffix}" CASCADE')

            promoted.append(table)

        # Clean up schemas
        cur.execute("DROP SCHEMA IF EXISTS _tombstone CASCADE")
        cur.execute("DROP SCHEMA IF EXISTS _local CASCADE")

    conn.close()
    return promoted


def setup_realtime_replication(
    local_pg: LocalPostgres,
    source_connstring: str,
    dbname: str,
    publication_name: str,
    subscription_name: str,
):
    """Set up logical replication subscription for real-time sync.

    Prerequisites:
    - Source must have wal_level=logical
    - Publication must exist on source
    - Local tables must exist (promoted from _local schema)

    The subscription uses copy_data=false because data is already local.
    """
    # Create publication on source
    source_conn = psycopg2.connect(source_connstring)
    source_conn.autocommit = True
    with source_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication_name,))
        if not cur.fetchone():
            cur.execute(f'CREATE PUBLICATION "{publication_name}" FOR ALL TABLES')
    source_conn.close()

    # Create subscription on local — copy_data=false since data is already here
    conn = psycopg2.connect(
        host="localhost", port=local_pg.port,
        user="mirrorbase", dbname=dbname,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_subscription WHERE subname = %s", (subscription_name,))
        if not cur.fetchone():
            cur.execute(f"""
                CREATE SUBSCRIPTION "{subscription_name}"
                CONNECTION '{source_connstring}'
                PUBLICATION "{publication_name}"
                WITH (
                    copy_data = false,
                    create_slot = true,
                    synchronous_commit = 'off'
                )
            """)
    conn.close()


def check_replication_status(local_pg: LocalPostgres, subscription_name: str, dbname: str) -> dict:
    """Check the status of the replication subscription."""
    conn = psycopg2.connect(
        host="localhost", port=local_pg.port,
        user="mirrorbase", dbname=dbname,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pid, received_lsn::text, latest_end_lsn::text, latest_end_time
            FROM pg_stat_subscription
            WHERE subname = %s
        """, (subscription_name,))
        row = cur.fetchone()
    conn.close()

    if not row:
        return {"status": "not_active", "pid": None}

    return {
        "status": "streaming",
        "pid": row[0],
        "received_lsn": row[1],
        "latest_end_lsn": row[2],
        "latest_end_time": row[3],
    }
