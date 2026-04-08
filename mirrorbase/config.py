import base64
import json
import os
import platform
import shutil
import socket
import subprocess
from pathlib import Path
from dataclasses import dataclass, asdict

from .exceptions import ConfigError


# ── Encryption for connection strings at rest ──

def _get_encryption_key() -> bytes | None:
    """Get encryption key from env. Returns None if not set (dev mode)."""
    key = os.environ.get("MIRRORBASE_ENCRYPTION_KEY")
    if not key:
        return None
    return key.encode()


def encrypt_value(value: str) -> str:
    """Encrypt a string value. Returns base64-encoded ciphertext prefixed with 'enc:'."""
    key = _get_encryption_key()
    if not key:
        return value  # Dev mode — no encryption

    from hashlib import sha256
    import hmac as _hmac

    # Simple XOR-based encryption with HMAC key derivation
    # For production, swap to Fernet/AES — but this avoids adding cryptography dep
    derived = sha256(key).digest()
    value_bytes = value.encode()
    encrypted = bytes(b ^ derived[i % len(derived)] for i, b in enumerate(value_bytes))
    tag = _hmac.new(key, encrypted, sha256).digest()[:16]
    return "enc:" + base64.b64encode(tag + encrypted).decode()


def decrypt_value(value: str) -> str:
    """Decrypt a string encrypted with encrypt_value."""
    if not value.startswith("enc:"):
        return value  # Not encrypted (dev mode or legacy)

    key = _get_encryption_key()
    if not key:
        raise ConfigError("MIRRORBASE_ENCRYPTION_KEY not set but encrypted data found")

    from hashlib import sha256
    import hmac as _hmac

    raw = base64.b64decode(value[4:])
    tag, encrypted = raw[:16], raw[16:]
    expected_tag = _hmac.new(key, encrypted, sha256).digest()[:16]
    if not _hmac.compare_digest(tag, expected_tag):
        raise ConfigError("Decryption failed — wrong key or corrupted data")

    derived = sha256(key).digest()
    decrypted = bytes(b ^ derived[i % len(derived)] for i, b in enumerate(encrypted))
    return decrypted.decode()

# ── All paths/settings configurable via env vars ──

MIRRORBASE_HOME = Path(os.environ.get("MIRRORBASE_HOME", Path.home() / ".mirrorbase"))
BASES_DIR = MIRRORBASE_HOME / "bases"
CLONES_DIR = MIRRORBASE_HOME / "clones"

PORT_RANGE_START = int(os.environ.get("MIRRORBASE_PORT_START", "5500"))
PORT_RANGE_END = int(os.environ.get("MIRRORBASE_PORT_END", "5999"))


def _find_pg_bin() -> Path:
    """Auto-detect Postgres binary directory. Checks in order:
    1. MIRRORBASE_PG_BIN env var
    2. Common Homebrew paths (macOS)
    3. System PATH (pg_ctl, initdb, psql)
    4. Common Linux paths
    """
    # Env var override
    env = os.environ.get("MIRRORBASE_PG_BIN")
    if env:
        p = Path(env)
        if (p / "pg_ctl").exists():
            return p
        raise ConfigError(f"MIRRORBASE_PG_BIN={env} does not contain pg_ctl")

    # Homebrew (macOS) — prefer latest version
    if platform.system() == "Darwin":
        for ver in range(18, 13, -1):
            p = Path(f"/opt/homebrew/opt/postgresql@{ver}/bin")
            if (p / "pg_ctl").exists():
                return p
        p = Path("/opt/homebrew/bin")
        if (p / "pg_ctl").exists():
            return p

    # System PATH
    pg_ctl = shutil.which("pg_ctl")
    if pg_ctl:
        return Path(pg_ctl).parent

    # Common Linux paths
    for path in [
        "/usr/lib/postgresql/17/bin",
        "/usr/lib/postgresql/16/bin",
        "/usr/lib/postgresql/15/bin",
        "/usr/lib/postgresql/14/bin",
        "/usr/bin",
    ]:
        p = Path(path)
        if (p / "pg_ctl").exists():
            return p

    raise ConfigError(
        "Could not find Postgres binaries. Install PostgreSQL or set MIRRORBASE_PG_BIN."
    )


PG_BIN = _find_pg_bin()


def _detect_cow_method() -> str:
    """Detect the best Copy-on-Write method for the current platform.

    Returns: 'apfs', 'zfs', 'btrfs', 'reflink', 'copy'
    """
    system = platform.system()

    if system == "Darwin":
        # macOS — APFS supports clonefile() via cp -c
        return "apfs"

    if system == "Linux":
        # Check ZFS
        if shutil.which("zfs"):
            return "zfs"

        # Check BTRFS
        try:
            result = subprocess.run(
                ["stat", "-f", "-c", "%T", str(MIRRORBASE_HOME)],
                capture_output=True, text=True,
            )
            if "btrfs" in result.stdout.lower():
                return "btrfs"
        except Exception:
            pass

        # Check XFS/ext4 reflinks — actually test on the real filesystem
        try:
            import tempfile
            test_dir = MIRRORBASE_HOME if MIRRORBASE_HOME.exists() else Path(tempfile.gettempdir())
            src = test_dir / ".mirrorbase_reflink_test"
            dst = test_dir / ".mirrorbase_reflink_test2"
            src.write_text("test")
            result = subprocess.run(
                ["cp", "--reflink=always", str(src), str(dst)],
                capture_output=True, text=True,
            )
            src.unlink(missing_ok=True)
            dst.unlink(missing_ok=True)
            if result.returncode == 0:
                return "reflink"
        except Exception:
            pass

    # Fallback — regular copy (not instant, but functional)
    return "copy"


COW_METHOD = _detect_cow_method()


@dataclass
class BaseMetadata:
    base_id: str
    source_connstring: str
    direct_connstring: str
    source_dbname: str
    port: int
    state: str
    created_at: str
    publication_name: str
    subscription_name: str
    sync_mode: str = "streaming"


MIRRORBASE_HOST = os.environ.get("MIRRORBASE_HOST", "localhost")


@dataclass
class CloneMetadata:
    clone_id: str
    base_id: str
    source_dbname: str
    port: int
    state: str
    created_at: str
    password: str = ""


def ensure_dirs():
    MIRRORBASE_HOME.mkdir(parents=True, exist_ok=True)
    BASES_DIR.mkdir(exist_ok=True)
    CLONES_DIR.mkdir(exist_ok=True)


def allocate_port() -> int:
    used_ports = set()
    for d in [BASES_DIR, CLONES_DIR]:
        if not d.exists():
            continue
        for sub in d.iterdir():
            meta_file = sub / "metadata.json"
            if meta_file.exists():
                meta = json.loads(meta_file.read_text())
                used_ports.add(meta.get("port"))

    for port in range(PORT_RANGE_START, PORT_RANGE_END + 1):
        if port in used_ports:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise ConfigError(f"No free ports in range {PORT_RANGE_START}-{PORT_RANGE_END}")


def save_metadata(path: Path, data):
    if hasattr(data, "__dict__"):
        data = asdict(data) if hasattr(data, "__dataclass_fields__") else data.__dict__
    # Encrypt sensitive fields before writing to disk
    sensitive_fields = ("source_connstring", "direct_connstring")
    for field in sensitive_fields:
        if field in data and data[field] and not data[field].startswith("enc:"):
            data[field] = encrypt_value(data[field])
    path.write_text(json.dumps(data, indent=2, default=str))


def load_metadata(path: Path) -> dict:
    data = json.loads(path.read_text())
    # Decrypt sensitive fields when reading
    sensitive_fields = ("source_connstring", "direct_connstring")
    for field in sensitive_fields:
        if field in data and data[field] and data[field].startswith("enc:"):
            data[field] = decrypt_value(data[field])
    return data
