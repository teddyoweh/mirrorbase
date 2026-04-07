"""
MirrorBase Example
==================

1. pip install mirrorbase
2. Replace the URL below with your Postgres connection string
3. python example.py

That's it.
"""

import mirrorbase
import psycopg2

# ── Replace this with your database URL ──
SOURCE_URL = "postgresql://user:password@host/dbname"

mb = mirrorbase.MirrorBase()

# Connect (instant — works with any Postgres, any size)
base_id = mb.connect(SOURCE_URL)

# Clone (instant — Copy-on-Write, <1 second for any DB size)
clone_id, clone_url = mb.clone(base_id)
print(f"Clone URL: {clone_url}")

# Use it — full SQL, reads and writes, source never touched
conn = psycopg2.connect(clone_url)
conn.autocommit = True
cur = conn.cursor()

# Read
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = [r[0] for r in cur.fetchall()]
print(f"Tables: {len(tables)}")

# Write (only affects clone)
cur.execute("CREATE TABLE test (id serial, msg text)")
cur.execute("INSERT INTO test (msg) VALUES ('hello from mirrorbase')")
cur.execute("SELECT * FROM test")
print(f"Wrote: {cur.fetchone()}")

conn.close()

# Cleanup
mb.teardown(base_id)
