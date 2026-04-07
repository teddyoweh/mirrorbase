"""Clone a database and run queries against it."""

import sys
import psycopg2
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])
_, url = mb.clone(base_id)

conn = psycopg2.connect(url)
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = [r[0] for r in cur.fetchall()]
print(f"{len(tables)} tables")

for t in tables[:5]:
    cur.execute(f'SELECT count(*) FROM "{t}"')
    print(f"  {t}: {cur.fetchone()[0]} rows")

conn.close()
mb.teardown(base_id)
