"""Tests for replicator module — URL detection, conversion, validation."""

from mirrorbase.replicator import (
    detect_pooler_url,
    convert_pooler_to_direct,
)


def test_detect_pooler_url_true():
    url = "postgresql://user:pass@ep-cool-name-123-pooler.us-east-2.aws.neon.tech/db"
    assert detect_pooler_url(url) is True


def test_detect_pooler_url_false():
    url = "postgresql://user:pass@ep-cool-name-123.us-east-2.aws.neon.tech/db"
    assert detect_pooler_url(url) is False


def test_detect_pooler_url_non_neon():
    url = "postgresql://user:pass@rds-instance.amazonaws.com/db"
    assert detect_pooler_url(url) is False


def test_detect_pooler_url_localhost():
    url = "postgresql://user:pass@localhost:5432/db"
    assert detect_pooler_url(url) is False


def test_convert_pooler_removes_pooler():
    pooler = "postgresql://user:pass@ep-cool-name-123-pooler.us-east-2.aws.neon.tech/db"
    direct = convert_pooler_to_direct(pooler)
    assert "-pooler" not in direct
    assert "ep-cool-name-123.us-east-2.aws.neon.tech" in direct


def test_convert_pooler_preserves_credentials():
    pooler = "postgresql://myuser:mypass@ep-abc-pooler.neon.tech/mydb"
    direct = convert_pooler_to_direct(pooler)
    assert "myuser" in direct
    assert "mypass" in direct
    assert "mydb" in direct


def test_convert_pooler_adds_sslmode():
    pooler = "postgresql://user:pass@ep-abc-pooler.neon.tech/db"
    direct = convert_pooler_to_direct(pooler)
    assert "sslmode=require" in direct


def test_convert_pooler_preserves_existing_sslmode():
    pooler = "postgresql://user:pass@ep-abc-pooler.neon.tech/db?sslmode=verify-full"
    direct = convert_pooler_to_direct(pooler)
    assert "verify-full" in direct
    assert direct.count("sslmode") == 1


def test_convert_non_pooler_unchanged():
    url = "postgresql://user:pass@regular-host.com/db"
    assert convert_pooler_to_direct(url) == url


def test_convert_pooler_preserves_query_params():
    pooler = "postgresql://user:pass@ep-abc-pooler.neon.tech/db?channel_binding=require&sslmode=require"
    direct = convert_pooler_to_direct(pooler)
    assert "channel_binding=require" in direct
    assert "sslmode=require" in direct
