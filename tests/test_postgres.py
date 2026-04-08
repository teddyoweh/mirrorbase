"""Tests for Postgres process management."""

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from mirrorbase.config import PG_BIN
from mirrorbase.postgres import LocalPostgres


@pytest.fixture
def pg_dirs():
    """Short paths for Postgres — Unix sockets have 107 char limit."""
    base = Path(tempfile.mkdtemp(prefix="mb_", dir="/tmp"))
    yield base / "data", base / "sock", base / "log"
    shutil.rmtree(base, ignore_errors=True)


def test_initdb_creates_data_dir(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5555, socket_dir, log_dir)
    pg.initdb()

    assert data_dir.exists()
    assert (data_dir / "postgresql.conf").exists()
    assert (data_dir / "pg_hba.conf").exists()


def test_postgresql_conf_has_overrides(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5556, socket_dir, log_dir)
    pg.initdb()

    conf = (data_dir / "postgresql.conf").read_text()
    assert "port = 5556" in conf
    assert "wal_level = logical" in conf
    assert "listen_addresses = 'localhost'" in conf


def test_pg_hba_has_trust(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5557, socket_dir, log_dir)
    pg.initdb()

    hba = (data_dir / "pg_hba.conf").read_text()
    assert "trust" in hba
    assert "127.0.0.1" in hba


def test_start_stop_cycle(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5558, socket_dir, log_dir)
    pg.initdb()

    assert not pg.is_running()
    pg.start()
    assert pg.is_running()

    result = pg.execute_sql("SELECT 1")
    assert result == [(1,)]

    pg.stop()
    assert not pg.is_running()


def test_execute_sql(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5559, socket_dir, log_dir)
    pg.initdb()
    pg.start()

    try:
        pg.execute_sql("CREATE DATABASE testdb")
        result = pg.execute_sql("SELECT datname FROM pg_database WHERE datname = 'testdb'")
        assert result == [("testdb",)]
    finally:
        pg.stop()


def test_clean_stale_pid(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5560, socket_dir, log_dir)
    pg.initdb()

    pid_file = data_dir / "postmaster.pid"
    pid_file.write_text("999999\n")
    pg._clean_stale_pid()
    assert not pid_file.exists()


def test_connstring():
    pg = LocalPostgres(Path("/tmp/test"), 5561, Path("/tmp/sock"), Path("/tmp/log"))
    cs = pg.connstring("mydb")
    assert "5561" in cs
    assert "mydb" in cs


def test_checkpoint(pg_dirs):
    data_dir, socket_dir, log_dir = pg_dirs
    pg = LocalPostgres(data_dir, 5562, socket_dir, log_dir)
    pg.initdb()
    pg.start()

    try:
        pg.checkpoint()
    finally:
        pg.stop()
