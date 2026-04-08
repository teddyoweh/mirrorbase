"""Tests for config module — paths, ports, encryption, CoW detection."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from mirrorbase.config import (
    allocate_port,
    save_metadata,
    load_metadata,
    encrypt_value,
    decrypt_value,
    _detect_cow_method,
    BaseMetadata,
    CloneMetadata,
)


def test_allocate_port_returns_int():
    port = allocate_port()
    assert isinstance(port, int)
    assert 5500 <= port <= 5999


def test_allocate_port_avoids_used(tmp_path):
    with patch("mirrorbase.config.BASES_DIR", tmp_path / "bases"), \
         patch("mirrorbase.config.CLONES_DIR", tmp_path / "clones"):
        (tmp_path / "bases").mkdir()
        (tmp_path / "clones").mkdir()

        # Create a fake base using port 5500
        base_dir = tmp_path / "bases" / "base-test"
        base_dir.mkdir()
        (base_dir / "metadata.json").write_text(json.dumps({"port": 5500}))

        port = allocate_port()
        assert port != 5500


def test_save_and_load_metadata(tmp_path):
    meta = BaseMetadata(
        base_id="test-1",
        source_connstring="postgresql://test",
        direct_connstring="postgresql://test",
        source_dbname="testdb",
        port=5500,
        state="ready",
        created_at="2026-01-01",
        publication_name="pub",
        subscription_name="sub",
    )
    path = tmp_path / "metadata.json"
    save_metadata(path, meta)
    loaded = load_metadata(path)
    assert loaded["base_id"] == "test-1"
    assert loaded["port"] == 5500
    assert loaded["state"] == "ready"


def test_clone_metadata(tmp_path):
    meta = CloneMetadata(
        clone_id="clone-1",
        base_id="base-1",
        source_dbname="testdb",
        port=5501,
        state="ready",
        created_at="2026-01-01",
        password="secret123",
    )
    path = tmp_path / "metadata.json"
    save_metadata(path, meta)
    loaded = load_metadata(path)
    assert loaded["clone_id"] == "clone-1"
    assert loaded["password"] == "secret123"


def test_encryption_roundtrip():
    with patch.dict(os.environ, {"MIRRORBASE_ENCRYPTION_KEY": "test-key-12345"}):
        original = "postgresql://user:secret@host/db"
        encrypted = encrypt_value(original)
        assert encrypted.startswith("enc:")
        assert "secret" not in encrypted
        decrypted = decrypt_value(encrypted)
        assert decrypted == original


def test_encryption_disabled_without_key():
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("MIRRORBASE_ENCRYPTION_KEY", None)
        original = "postgresql://user:secret@host/db"
        result = encrypt_value(original)
        assert result == original  # no encryption without key


def test_cow_detection():
    method = _detect_cow_method()
    assert method in ("apfs", "zfs", "btrfs", "reflink", "copy")


def test_metadata_encrypts_connstrings(tmp_path):
    with patch.dict(os.environ, {"MIRRORBASE_ENCRYPTION_KEY": "test-key-xyz"}):
        meta = {"source_connstring": "postgresql://secret", "direct_connstring": "postgresql://secret2", "port": 5500}
        path = tmp_path / "meta.json"
        save_metadata(path, meta)

        # Raw file should have encrypted values
        raw = json.loads(path.read_text())
        assert raw["source_connstring"].startswith("enc:")
        assert raw["direct_connstring"].startswith("enc:")

        # load_metadata should decrypt
        loaded = load_metadata(path)
        assert loaded["source_connstring"] == "postgresql://secret"
        assert loaded["direct_connstring"] == "postgresql://secret2"
