class MirrorBaseError(Exception):
    """Base exception for all MirrorBase errors."""

class PostgresError(MirrorBaseError):
    """Errors related to Postgres process management."""

class ReplicationError(MirrorBaseError):
    """Errors related to replication setup or monitoring."""

class CloneError(MirrorBaseError):
    """Errors related to CoW clone operations."""

class ConfigError(MirrorBaseError):
    """Errors related to configuration or storage."""
