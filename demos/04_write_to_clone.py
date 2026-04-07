"""Write to a clone without affecting the source."""

import sys
import psycopg2
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])
_, url = mb.clone(base_id)

conn = psycopg2.connect(url)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE TABLE test (id serial, msg text)")
cur.execute("INSERT INTO test (msg) VALUES ('hello from clone')")
cur.execute("SELECT * FROM test")
print(f"Clone: {cur.fetchone()}")
print("Source: untouched")

conn.close()
mb.teardown(base_id)
