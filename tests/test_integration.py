"""Integration tests — full connect → clone → query → destroy cycle.

These tests require a running Postgres and a real source database URL.
Set MIRRORBASE_TEST_URL to run them:

    MIRRORBASE_TEST_URL="postgresql://user:pass@host/db" pytest tests/test_integration.py
"""

import os
import time

import psycopg2
import pytest

import mirrorbase

TEST_URL = os.environ.get("MIRRORBASE_TEST_URL")


@pytest.fixture
def mb():
    m = mirrorbase.MirrorBase()
    yield m


@pytest.mark.skipif(not TEST_URL, reason="MIRRORBASE_TEST_URL not set")
def test_connect(mb):
    base_id = mb.connect(TEST_URL)
    assert base_id.startswith("base-")
    status = mb.status(base_id)
    assert status["state"] == "ready"
    assert status["running"] is True
    mb.teardown(base_id)


@pytest.mark.skipif(not TEST_URL, reason="MIRRORBASE_TEST_URL not set")
def test_clone(mb):
    base_id = mb.connect(TEST_URL)
    clone_id, url = mb.clone(base_id)
    assert clone_id.startswith("clone-")
    assert "postgresql://" in url

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)
    conn.close()

    mb.teardown(base_id)


@pytest.mark.skipif(not TEST_URL, reason="MIRRORBASE_TEST_URL not set")
def test_clone_write_isolation(mb):
    base_id = mb.connect(TEST_URL)
    _, url1 = mb.clone(base_id, clone_id="iso-1")
    _, url2 = mb.clone(base_id, clone_id="iso-2")

    # Write to clone 1
    conn1 = psycopg2.connect(url1)
    conn1.autocommit = True
    conn1.cursor().execute("CREATE TABLE isolation_test (id serial, val text)")
    conn1.cursor().execute("INSERT INTO isolation_test (val) VALUES ('clone-1')")
    conn1.close()

    # Clone 2 should not have it
    conn2 = psycopg2.connect(url2)
    conn2.autocommit = True
    cur2 = conn2.cursor()
    try:
        cur2.execute("SELECT 1 FROM isolation_test")
        assert False, "clone-2 should not have isolation_test"
    except Exception:
        pass
    conn2.close()

    mb.teardown(base_id)


@pytest.mark.skipif(not TEST_URL, reason="MIRRORBASE_TEST_URL not set")
def test_multiple_clones(mb):
    base_id = mb.connect(TEST_URL)

    clones = []
    for i in range(3):
        cid, url = mb.clone(base_id, clone_id=f"multi-{i}")
        clones.append((cid, url))

    assert len(mb.list_clones()) == 3

    for cid, url in clones:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        conn.cursor().execute("SELECT 1")
        conn.close()

    mb.teardown(base_id)


@pytest.mark.skipif(not TEST_URL, reason="MIRRORBASE_TEST_URL not set")
def test_destroy_single_clone(mb):
    base_id = mb.connect(TEST_URL)
    cid1, _ = mb.clone(base_id, clone_id="keep")
    cid2, _ = mb.clone(base_id, clone_id="destroy-me")

    assert len(mb.list_clones()) == 2
    mb.destroy("destroy-me")
    assert len(mb.list_clones()) == 1
    assert mb.list_clones()[0]["clone_id"] == "keep"

    mb.teardown(base_id)


@pytest.mark.skipif(not TEST_URL, reason="MIRRORBASE_TEST_URL not set")
def test_complex_queries(mb):
    base_id = mb.connect(TEST_URL)
    _, url = mb.clone(base_id)

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    # CTE
    cur.execute("WITH n AS (SELECT 1 x) SELECT x FROM n")
    assert cur.fetchone() == (1,)

    # Recursive
    cur.execute("WITH RECURSIVE r AS (SELECT 1 n UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT count(*) FROM r")
    assert cur.fetchone() == (5,)

    # Window function
    cur.execute("SELECT ROW_NUMBER() OVER () FROM generate_series(1,3)")
    assert len(cur.fetchall()) == 3

    conn.close()
    mb.teardown(base_id)
