"""Tests for CoW clone operations."""

import platform
from pathlib import Path

from mirrorbase.clone import _cow_clone, _patch_clone_config
from mirrorbase.exceptions import CloneError
from mirrorbase.config import COW_METHOD


def test_cow_method_detected():
    """Platform should have a CoW method."""
    assert COW_METHOD in ("apfs", "zfs", "btrfs", "reflink", "copy")
    if platform.system() == "Darwin":
        assert COW_METHOD == "apfs"


def test_cow_clone_creates_copy(tmp_path):
    """CoW clone should create an identical directory."""
    src = tmp_path / "source"
    src.mkdir()
    (src / "file.txt").write_text("hello")
    (src / "subdir").mkdir()
    (src / "subdir" / "nested.txt").write_text("world")

    dst = tmp_path / "clone"
    _cow_clone(src, dst)

    assert dst.exists()
    assert (dst / "file.txt").read_text() == "hello"
    assert (dst / "subdir" / "nested.txt").read_text() == "world"


def test_cow_clone_is_independent(tmp_path):
    """Writing to clone should not affect source."""
    src = tmp_path / "source"
    src.mkdir()
    (src / "data.txt").write_text("original")

    dst = tmp_path / "clone"
    _cow_clone(src, dst)

    # Modify clone
    (dst / "data.txt").write_text("modified")
    (dst / "new_file.txt").write_text("only in clone")

    # Source unchanged
    assert (src / "data.txt").read_text() == "original"
    assert not (src / "new_file.txt").exists()


def test_patch_clone_config(tmp_path):
    """Patching config should append port and socket overrides."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    conf = data_dir / "postgresql.conf"
    conf.write_text("# original config\nport = 5500\n")

    _patch_clone_config(data_dir, 5501, tmp_path / "socket", tmp_path / "log")

    content = conf.read_text()
    assert "port = 5501" in content
    assert "MirrorBase clone overrides" in content
