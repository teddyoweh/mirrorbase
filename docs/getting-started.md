# Getting Started

## Install

```bash
pip install mirrorbase
```

Requires PostgreSQL 14+ on the machine.

## 1. As a Python Library

```python
import mirrorbase

mb = mirrorbase.MirrorBase()
base_id = mb.connect("postgresql://user:pass@host/dbname")
clone_id, url = mb.clone(base_id)

# url → postgresql://clone_user:pass@localhost:5501/dbname
# Use it with psycopg2, SQLAlchemy, anything that speaks Postgres.
```

## 2. As a CLI

```bash
mirrorbase connect "postgresql://user:pass@host/dbname"
# → base-abc123

mirrorbase clone base-abc123
# → clone-xyz: postgresql://clone_user:pass@localhost:5501/dbname

mirrorbase list
mirrorbase destroy clone-xyz
mirrorbase teardown base-abc123
```

## 3. As a Server

```bash
export MIRRORBASE_API_KEY=your-secret-key
mirrorbase serve
# → Running at http://0.0.0.0:8100
```

Hit the root URL to see all endpoints:

```bash
curl http://localhost:8100/
```
