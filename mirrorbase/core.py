import shutil
import time
import uuid
from pathlib import Path
from typing import Optional, Callable

import psycopg2

from .config import (
    BASES_DIR, CLONES_DIR, BaseMetadata,
    ensure_dirs, allocate_port, save_metadata, load_metadata,
)
from .postgres import LocalPostgres
from .replicator import (
    detect_pooler_url, convert_pooler_to_direct, validate_source_connection,
)
from .fdw import setup_fdw, BackgroundMigrator
from .clone import create_clone, destroy_clone, list_clones as _list_clones
from .exceptions import MirrorBaseError


class MirrorBase:

    def __init__(self):
        self._migrators: dict = {}

    def connect(
        self,
        connstring: str,
        on_progress: Optional[Callable] = None,
    ) -> str:
        """Connect to a source database. Available instantly, streams in background.

        Flow:
        1. FDW proxy to source (instant — zero data transfer)
        2. Local overlay for writes (INSERT/UPDATE/DELETE never touch source)
        3. Background streaming copies tables locally via parallel COPY
        4. As tables finish, reads swap from FDW to local (faster, less source load)
        5. Once fully local, source only sees WAL-level replication (if available)

        Works with ANY Postgres. No config changes required on source.
        """
        ensure_dirs()

        direct_connstring = connstring
        if detect_pooler_url(connstring):
            direct_connstring = convert_pooler_to_direct(connstring)

        source_info = validate_source_connection(direct_connstring)
        source_dbname = source_info["dbname"]

        base_id = f"base-{uuid.uuid4().hex[:8]}"
        base_dir = BASES_DIR / base_id
        data_dir = base_dir / "data"
        log_dir = base_dir / "log"
        socket_dir = base_dir / "socket"
        port = allocate_port()

        metadata = BaseMetadata(
            base_id=base_id,
            source_connstring=connstring,
            direct_connstring=direct_connstring,
            source_dbname=source_dbname,
            port=port,
            state="initializing",
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            publication_name="",
            subscription_name="",
            sync_mode="streaming",
        )
        base_dir.mkdir(parents=True)
        save_metadata(base_dir / "metadata.json", metadata)

        try:
            local_pg = LocalPostgres(data_dir, port, socket_dir, log_dir)
            local_pg.initdb()
            local_pg.start()

            # Create target database
            local_pg.execute_sql(f'CREATE DATABASE "{source_dbname}"')

            # Set up FDW + local overlay — instant, zero data transfer
            tables = setup_fdw(local_pg, direct_connstring, source_dbname, source_dbname)

            metadata.state = "ready"
            save_metadata(base_dir / "metadata.json", metadata)

            # Start background streaming — copies tables locally via parallel COPY
            # Once complete: promote tables + start real-time replication
            migrator = BackgroundMigrator(
                source_connstring=direct_connstring,
                local_pg=local_pg,
                local_dbname=source_dbname,
                tables=tables,
                logical_available=source_info["logical_available"],
                on_progress=on_progress,
            )
            migrator.start()
            self._migrators[base_id] = migrator

            return base_id

        except Exception:
            metadata.state = "error"
            save_metadata(base_dir / "metadata.json", metadata)
            raise

    def clone(self, base_id: str, clone_id: str | None = None) -> tuple[str, str]:
        """Create instant CoW clone. Returns (clone_id, connection_string).

        Clone is available immediately regardless of background migration state.
        - Tables already migrated: reads are fully local (zero source load)
        - Tables still streaming: reads proxy through FDW (standard replication load)
        - Writes: always local, never touch source
        """
        base_dir = BASES_DIR / base_id
        if not base_dir.exists():
            raise MirrorBaseError(f"Base {base_id} not found")

        meta = load_metadata(base_dir / "metadata.json")

        base_pg = LocalPostgres(
            data_dir=base_dir / "data",
            port=meta["port"],
            socket_dir=base_dir / "socket",
            log_dir=base_dir / "log",
        )

        cid, clone_pg = create_clone(
            base_pg=base_pg,
            base_id=base_id,
            base_data_dir=base_dir / "data",
            source_dbname=meta["source_dbname"],
            clone_id=clone_id,
        )

        connstr = clone_pg.connstring(meta["source_dbname"])
        return cid, connstr

    def migration_status(self, base_id: str) -> dict:
        """Check how much data has been migrated locally."""
        migrator = self._migrators.get(base_id)
        if not migrator:
            return {"status": "complete", "migrated": "all", "remaining": 0}
        return {
            "status": "streaming",
            "migrated": len(migrator.migrated),
            "total": len(migrator.tables),
            "remaining": len(migrator.tables) - len(migrator.migrated),
            "tables_local": list(migrator.migrated),
        }

    def status(self, base_id: str) -> dict:
        base_dir = BASES_DIR / base_id
        if not base_dir.exists():
            raise MirrorBaseError(f"Base {base_id} not found")
        meta = load_metadata(base_dir / "metadata.json")

        base_pg = LocalPostgres(
            data_dir=base_dir / "data",
            port=meta["port"],
            socket_dir=base_dir / "socket",
            log_dir=base_dir / "log",
        )

        result = {
            "base_id": base_id,
            "state": meta["state"],
            "port": meta["port"],
            "running": base_pg.is_running(),
            "source": meta["source_connstring"],
            "connstring": f"postgresql://mirrorbase@localhost:{meta['port']}/{meta['source_dbname']}",
            "sync_mode": meta.get("sync_mode", "unknown"),
            "migration": self.migration_status(base_id),
        }

        return result

    def list_bases(self) -> list[dict]:
        ensure_dirs()
        results = []
        for base_dir in sorted(BASES_DIR.iterdir()):
            meta_file = base_dir / "metadata.json"
            if meta_file.exists():
                results.append(load_metadata(meta_file))
        return results

    def list_clones(self, base_id: str | None = None) -> list[dict]:
        ensure_dirs()
        return _list_clones(base_id=base_id)

    def destroy(self, clone_id: str):
        destroy_clone(clone_id)

    def teardown(self, base_id: str):
        """Tear down base replica and all its clones."""
        if base_id in self._migrators:
            self._migrators[base_id].stop()
            del self._migrators[base_id]
        base_dir = BASES_DIR / base_id
        if not base_dir.exists():
            raise MirrorBaseError(f"Base {base_id} not found")
        meta = load_metadata(base_dir / "metadata.json")

        # Destroy all clones first
        for clone_meta in self.list_clones(base_id=base_id):
            try:
                self.destroy(clone_meta["clone_id"])
            except Exception:
                pass

        base_pg = LocalPostgres(
            data_dir=base_dir / "data",
            port=meta["port"],
            socket_dir=base_dir / "socket",
            log_dir=base_dir / "log",
        )

        if base_pg.is_running():
            base_pg.stop()

        shutil.rmtree(base_dir)
