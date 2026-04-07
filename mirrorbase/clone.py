import secrets
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import psycopg2

from .config import CLONES_DIR, MIRRORBASE_HOST, CloneMetadata, allocate_port, save_metadata, load_metadata
from .postgres import LocalPostgres
from .exceptions import CloneError


def create_clone(
    base_pg: LocalPostgres,
    base_id: str,
    base_data_dir: Path,
    source_dbname: str,
    clone_id: str | None = None,
) -> tuple[str, LocalPostgres]:
    if clone_id is None:
        clone_id = f"clone-{uuid.uuid4().hex[:8]}"

    clone_dir = CLONES_DIR / clone_id
    clone_data_dir = clone_dir / "data"
    clone_log_dir = clone_dir / "log"
    clone_socket_dir = clone_dir / "socket"
    clone_port = allocate_port()

    clone_dir.mkdir(parents=True, exist_ok=True)
    clone_log_dir.mkdir(exist_ok=True)
    clone_socket_dir.mkdir(exist_ok=True)

    try:
        # 1. Checkpoint to flush dirty pages
        base_pg.checkpoint()

        # 2. Stop base Postgres
        base_pg.stop(mode="fast")

        # 3. CoW clone — instant regardless of data size
        _cow_clone(base_data_dir, clone_data_dir)

        # 4. Restart base immediately
        base_pg.start()

        # 5. Patch clone config (new port, socket dir, log dir)
        _patch_clone_config(clone_data_dir, clone_port, clone_socket_dir, clone_log_dir)

        # 6. Remove stale postmaster.pid
        stale_pid = clone_data_dir / "postmaster.pid"
        if stale_pid.exists():
            stale_pid.unlink()

        # 7. Start clone Postgres
        clone_pg = LocalPostgres(
            data_dir=clone_data_dir,
            port=clone_port,
            socket_dir=clone_socket_dir,
            log_dir=clone_log_dir,
        )
        clone_pg.start()

        # 8. Drop inherited subscription to avoid replication conflicts
        _drop_inherited_subscriptions(clone_pg, source_dbname)

        # 9. Create a dedicated user with random password for this clone
        clone_password = secrets.token_urlsafe(24)
        clone_user = f"clone_{clone_id.replace('-', '_')}"
        clone_pg.execute_sql(f"CREATE USER \"{clone_user}\" WITH PASSWORD '{clone_password}'")
        clone_pg.execute_sql(f'GRANT ALL PRIVILEGES ON DATABASE "{source_dbname}" TO "{clone_user}"', dbname="postgres")
        # Grant on all schemas/tables
        clone_pg.execute_sql(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{clone_user}"', dbname=source_dbname)
        clone_pg.execute_sql(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{clone_user}"', dbname=source_dbname)
        clone_pg.execute_sql(f'GRANT USAGE, CREATE ON SCHEMA public TO "{clone_user}"', dbname=source_dbname)
        # For FDW schemas if they exist
        for schema in ["_local", "_fdw", "_tombstone"]:
            try:
                clone_pg.execute_sql(f'GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO "{clone_user}"', dbname=source_dbname)
                clone_pg.execute_sql(f'GRANT USAGE ON SCHEMA {schema} TO "{clone_user}"', dbname=source_dbname)
            except Exception:
                pass

        # 10. Save metadata
        metadata = CloneMetadata(
            clone_id=clone_id,
            base_id=base_id,
            source_dbname=source_dbname,
            port=clone_port,
            state="ready",
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            password=clone_password,
        )
        save_metadata(clone_dir / "metadata.json", metadata)

        # Build the connection string with auth
        clone_pg._clone_user = clone_user
        clone_pg._clone_password = clone_password

        return clone_id, clone_pg

    except Exception as e:
        # Ensure base is running on error
        if not base_pg.is_running():
            try:
                base_pg.start()
            except Exception:
                pass
        # Clean up partial clone
        if clone_dir.exists():
            # Stop clone if it started
            try:
                clone_pg_cleanup = LocalPostgres(clone_data_dir, clone_port, clone_socket_dir, clone_log_dir)
                if clone_pg_cleanup.is_running():
                    clone_pg_cleanup.stop()
            except Exception:
                pass
            shutil.rmtree(clone_dir, ignore_errors=True)
        raise CloneError(f"Clone creation failed: {e}") from e


def _cow_clone(src: Path, dst: Path):
    """Copy-on-Write clone using the best available method.

    - macOS APFS: cp -c (clonefile syscall)
    - Linux ZFS: zfs clone from snapshot
    - Linux BTRFS: btrfs subvolume snapshot
    - Linux XFS/ext4: cp --reflink=always
    - Fallback: regular cp -r (not instant, but works)
    """
    from .config import COW_METHOD

    if COW_METHOD == "apfs":
        result = subprocess.run(
            ["cp", "-c", "-r", str(src), str(dst)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise CloneError(f"APFS clone failed: {result.stderr}")

    elif COW_METHOD == "zfs":
        # ZFS: create snapshot then clone
        # Detect the ZFS dataset from the source path
        result = subprocess.run(
            ["zfs", "list", "-H", "-o", "name", "-s", "name"],
            capture_output=True, text=True,
        )
        # Find dataset for src path
        dataset = None
        for line in result.stdout.strip().split("\n"):
            ds = line.strip()
            mp_result = subprocess.run(
                ["zfs", "get", "-H", "-o", "value", "mountpoint", ds],
                capture_output=True, text=True,
            )
            mp = mp_result.stdout.strip()
            if str(src).startswith(mp):
                dataset = ds
        if not dataset:
            raise CloneError(f"Could not find ZFS dataset for {src}")

        snap_name = f"{dataset}@mirrorbase_{dst.name}"
        clone_dataset = f"{dataset.rsplit('/', 1)[0]}/{dst.name}"
        subprocess.run(["zfs", "snapshot", snap_name], check=True, capture_output=True)
        subprocess.run(["zfs", "clone", snap_name, clone_dataset], check=True, capture_output=True)

    elif COW_METHOD == "btrfs":
        result = subprocess.run(
            ["btrfs", "subvolume", "snapshot", str(src), str(dst)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise CloneError(f"BTRFS snapshot failed: {result.stderr}")

    elif COW_METHOD == "reflink":
        result = subprocess.run(
            ["cp", "--reflink=always", "-r", str(src), str(dst)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise CloneError(f"Reflink clone failed: {result.stderr}")

    else:
        # Fallback: regular copy (not instant)
        import shutil as _shutil
        _shutil.copytree(str(src), str(dst))


def _patch_clone_config(data_dir: Path, port: int, socket_dir: Path, log_dir: Path):
    conf_path = data_dir / "postgresql.conf"
    with open(conf_path, "a") as f:
        f.write(f"""

# === MirrorBase clone overrides ===
port = {port}
unix_socket_directories = '{socket_dir}'
log_directory = '{log_dir}'
""")


def _drop_inherited_subscriptions(clone_pg: LocalPostgres, source_dbname: str):
    """Drop subscriptions inherited from base to avoid replication conflicts.

    Critical sequence:
    1. ALTER SUBSCRIPTION ... DISABLE (stop the worker)
    2. ALTER SUBSCRIPTION ... SET (slot_name = NONE) (detach from remote slot)
    3. DROP SUBSCRIPTION ... (remove without touching remote)
    """
    databases = clone_pg.execute_sql(
        "SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres'"
    )
    for (dbname,) in (databases or []):
        try:
            conn = psycopg2.connect(host="localhost", port=clone_pg.port, user="mirrorbase", dbname=dbname)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT subname FROM pg_subscription")
                subs = cur.fetchall()
                for (subname,) in subs:
                    cur.execute(f'ALTER SUBSCRIPTION "{subname}" DISABLE')
                    cur.execute(f'ALTER SUBSCRIPTION "{subname}" SET (slot_name = NONE)')
                    cur.execute(f'DROP SUBSCRIPTION "{subname}"')
            conn.close()
        except Exception:
            pass


def destroy_clone(clone_id: str):
    clone_dir = CLONES_DIR / clone_id
    if not clone_dir.exists():
        raise CloneError(f"Clone {clone_id} does not exist")

    meta = load_metadata(clone_dir / "metadata.json")

    clone_pg = LocalPostgres(
        data_dir=clone_dir / "data",
        port=meta["port"],
        socket_dir=clone_dir / "socket",
        log_dir=clone_dir / "log",
    )

    if clone_pg.is_running():
        clone_pg.stop(mode="fast")

    shutil.rmtree(clone_dir)


def list_clones(base_id: str | None = None) -> list[dict]:
    if not CLONES_DIR.exists():
        return []
    results = []
    for clone_dir in sorted(CLONES_DIR.iterdir()):
        meta_file = clone_dir / "metadata.json"
        if meta_file.exists():
            meta = load_metadata(meta_file)
            if base_id is None or meta.get("base_id") == base_id:
                results.append(meta)
    return results
