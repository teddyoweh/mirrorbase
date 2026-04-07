"""Give an AI agent a safe database sandbox."""

import sys
import psycopg2
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])
_, url = mb.clone(base_id, clone_id="agent-session")

conn = psycopg2.connect(url)
conn.autocommit = True
cur = conn.cursor()

# Agent can do anything — source is never touched
cur.execute("CREATE TABLE agent_output (id serial, finding text)")
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
for (table,) in cur.fetchall():
    try:
        cur.execute(f'SELECT count(*) FROM "{table}"')
        count = cur.fetchone()[0]
        if count > 0:
            cur.execute("INSERT INTO agent_output (finding) VALUES (%s)", (f"{table}: {count} rows",))
    except Exception:
        pass

cur.execute("SELECT finding FROM agent_output ORDER BY id")
print("Agent findings:")
for (f,) in cur.fetchall():
    print(f"  {f}")

conn.close()
mb.teardown(base_id)
