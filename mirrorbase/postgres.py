import os
import subprocess
import time
from pathlib import Path

import psycopg2

from .config import PG_BIN
from .exceptions import PostgresError


class LocalPostgres:
    """Manages a single Postgres instance with a custom data directory."""

    def __init__(self, data_dir: Path, port: int, socket_dir: Path, log_dir: Path):
        self.data_dir = data_dir
        self.port = port
        self.socket_dir = socket_dir
        self.log_dir = log_dir

    @property
    def log_file(self) -> Path:
        return self.log_dir / "postgresql.log"

    def connstring(self, dbname: str = "postgres") -> str:
        from .config import MIRRORBASE_HOST
        user = getattr(self, "_clone_user", "mirrorbase")
        password = getattr(self, "_clone_password", None)
        if password:
            return f"postgresql://{user}:{password}@{MIRRORBASE_HOST}:{self.port}/{dbname}"
        return f"postgresql://mirrorbase@{MIRRORBASE_HOST}:{self.port}/{dbname}"

    def initdb(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.socket_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        result = subprocess.run(
            [
                str(PG_BIN / "initdb"),
                "-D", str(self.data_dir),
                "-A", "trust",
                "-U", "mirrorbase",
                "--no-locale",
                "--encoding=UTF8",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise PostgresError(f"initdb failed: {result.stderr}")

        self._write_postgresql_conf()
        self._write_pg_hba_conf()

    def _write_postgresql_conf(self):
        conf_path = self.data_dir / "postgresql.conf"
        existing = conf_path.read_text()
        overrides = f"""
# === MirrorBase overrides ===
listen_addresses = 'localhost'
port = {self.port}
unix_socket_directories = '{self.socket_dir}'

# Logical replication (needed as subscriber)
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_logical_replication_workers = 4
max_sync_workers_per_subscription = 2
max_worker_processes = 10

# Performance
shared_buffers = 128MB
work_mem = 16MB
maintenance_work_mem = 128MB

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = '{self.log_dir}'
log_filename = 'postgresql.log'
log_min_messages = warning
"""
        conf_path.write_text(existing + overrides)

    def _write_pg_hba_conf(self):
        hba_path = self.data_dir / "pg_hba.conf"
        hba_path.write_text(
            "# MirrorBase: trust all local connections\n"
            "local   all             all                     trust\n"
            "host    all             all     127.0.0.1/32    trust\n"
            "host    all             all     ::1/128         trust\n"
            "local   replication     all                     trust\n"
            "host    replication     all     127.0.0.1/32    trust\n"
            "host    replication     all     ::1/128         trust\n"
        )

    def _clean_stale_pid(self):
        pid_file = self.data_dir / "postmaster.pid"
        if not pid_file.exists():
            return
        lines = pid_file.read_text().strip().split("\n")
        if not lines:
            pid_file.unlink()
            return
        try:
            pid = int(lines[0])
            os.kill(pid, 0)
        except (ValueError, ProcessLookupError):
            pid_file.unlink()
        except PermissionError:
            pass  # process exists

    def start(self, wait_timeout: int = 30):
        self._clean_stale_pid()
        result = subprocess.run(
            [
                str(PG_BIN / "pg_ctl"),
                "start",
                "-D", str(self.data_dir),
                "-l", str(self.log_file),
                "-w",
                "-t", str(wait_timeout),
                "-o", f"-p {self.port} -k {self.socket_dir}",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise PostgresError(f"Failed to start Postgres on port {self.port}: {result.stderr}\n{result.stdout}")
        self._wait_for_ready(timeout=wait_timeout)

    def _wait_for_ready(self, timeout: int = 30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = subprocess.run(
                [str(PG_BIN / "pg_isready"), "-h", "localhost", "-p", str(self.port)],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                return
            time.sleep(0.2)
        raise PostgresError(f"Postgres on port {self.port} not ready within {timeout}s")

    def stop(self, mode: str = "fast"):
        if not self.is_running():
            return
        result = subprocess.run(
            [
                str(PG_BIN / "pg_ctl"),
                "stop",
                "-D", str(self.data_dir),
                "-m", mode,
                "-w",
                "-t", "30",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise PostgresError(f"Failed to stop Postgres: {result.stderr}")

    def is_running(self) -> bool:
        result = subprocess.run(
            [str(PG_BIN / "pg_ctl"), "status", "-D", str(self.data_dir)],
            capture_output=True, text=True,
        )
        return result.returncode == 0

    def execute_sql(self, sql: str, dbname: str = "postgres", fetch: bool = True):
        conn = psycopg2.connect(host="localhost", port=self.port, user="mirrorbase", dbname=dbname)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                if fetch and cur.description:
                    return cur.fetchall()
                return None
        finally:
            conn.close()

    def checkpoint(self):
        self.execute_sql("CHECKPOINT;")
