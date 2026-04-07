"""Run JOINs, CTEs, window functions, subqueries on a clone."""

import sys
import psycopg2
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect(sys.argv[1])
_, url = mb.clone(base_id)

conn = psycopg2.connect(url)
conn.autocommit = True
cur = conn.cursor()

queries = [
    ("CTE",           "WITH cte AS (SELECT * FROM users) SELECT count(*) FROM cte"),
    ("WINDOW",        "SELECT id, ROW_NUMBER() OVER () FROM users LIMIT 3"),
    ("RECURSIVE",     "WITH RECURSIVE n AS (SELECT 1 x UNION ALL SELECT x+1 FROM n WHERE x<5) SELECT array_agg(x) FROM n"),
    ("SUBQUERY",      "SELECT count(*) FROM users WHERE id IN (SELECT user_id FROM wallets)"),
    ("CROSS JOIN",    "SELECT count(*) FROM users, agents"),
    ("CASE",          "SELECT CASE WHEN count(*)>0 THEN 'has data' ELSE 'empty' END FROM users"),
    ("JSON",          "SELECT json_build_object('users', count(*)) FROM users"),
    ("STRING",        "SELECT upper(name), length(name) FROM users LIMIT 1"),
]

for name, sql in queries:
    try:
        cur.execute(sql)
        print(f"  PASS  {name}: {cur.fetchone()}")
    except Exception as e:
        print(f"  FAIL  {name}: {e}")

conn.close()
mb.teardown(base_id)
