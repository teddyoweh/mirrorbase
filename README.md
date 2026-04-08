# MirrorBase

Instant live clones of Postgres databases. Paste a URL, get a writable fork in under 4 seconds — from 0 GB to 10 TB.

```
Connect + Clone:  < 4s   (constant time — 0 GB to 10 TB)
Clone only:       ~0.6s  (Copy-on-Write, no data moves)
Queries:          Full SQL — JOINs, CTEs, window functions, everything
Writes:           Fully isolated — clones never touch the source
```

Clone time is O(1). It's a metadata operation — the same 0.6 seconds whether your database is 10 MB or 10 TB.

## Install

```bash
pip install mirrorbase
```

Requires PostgreSQL 14+ installed locally.

## Quickstart

### Python API

```python
import mirrorbase

mb = mirrorbase.MirrorBase()

# Connect to any Postgres database
base_id = mb.connect("postgresql://user:pass@host/dbname")

# Create an instant clone
clone_id, url = mb.clone(base_id)
# url = "postgresql://clone_user:rAnDoMpAsS@your-server:6001/dbname"

# Use it — full read/write, source never touched
import psycopg2
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute("SELECT * FROM users")         # reads work
cur.execute("DROP TABLE users")            # writes only affect clone
conn.close()

# Cleanup
mb.destroy(clone_id)
mb.teardown(base_id)
```

### CLI

```bash
mirrorbase connect "postgresql://user:pass@host/dbname"
mirrorbase clone base-xxxxx --name my-clone
mirrorbase list
mirrorbase destroy my-clone
mirrorbase teardown base-xxxxx
```

### REST API

```bash
# Start server
export MIRRORBASE_API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
mirrorbase serve
```

```bash
# Connect a source database
curl -X POST https://mirrorbase.yourserver.com:8100/connect \
  -H "Authorization: Bearer $MIRRORBASE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"url": "postgresql://user:pass@customer-host/dbname"}'

# → {"base_id": "base-abc123", "connstring": "postgresql://...", "elapsed": 2.1}
```

```bash
# Create a clone
curl -X POST https://mirrorbase.yourserver.com:8100/clone \
  -H "Authorization: Bearer $MIRRORBASE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"base_id": "base-abc123"}'

# → {"clone_id": "clone-xyz", "connstring": "postgresql://clone_user:pass@mirrorbase.yourserver.com:6001/dbname", "elapsed": 0.6}
```

```bash
# Destroy a clone
curl -X DELETE https://mirrorbase.yourserver.com:8100/clones/clone-xyz \
  -H "Authorization: Bearer $MIRRORBASE_API_KEY"
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check (no auth) |
| `POST` | `/connect` | Connect a source database |
| `POST` | `/clone` | Create a clone |
| `GET` | `/bases` | List all connected databases |
| `GET` | `/bases/:id` | Status + migration progress |
| `GET` | `/clones` | List all clones |
| `DELETE` | `/clones/:id` | Destroy a clone |
| `DELETE` | `/bases/:id` | Teardown base + all clones |

## How the URLs Work

There are three URLs involved:

```
1. SOURCE URL (customer gives you this)
   postgresql://user:pass@their-server.neon.tech/their_db
   ↓
   Customer's production database. MirrorBase reads from it.
   You never expose this to agents.

2. API URL (your MirrorBase server, HTTP)
   https://mirrorbase.yourserver.com:8100
   ↓
   Your backend calls this to manage connections and clones.
   Protected by API key.

3. CLONE URL (MirrorBase returns this, Postgres protocol)
   postgresql://clone_abc:rAnDoMpAsS@mirrorbase.yourserver.com:6001/their_db
   ↓
   What the agent connects to. Unique user + random password per clone.
   Full read/write. Source is never touched.
```

**Flow:**
```
Your backend                          MirrorBase server
                                      (mirrorbase.yourserver.com)

POST /connect {"url": <SOURCE>}  →    Streams data from source
                                  ←    {"base_id": "base-xxx"}

POST /clone {"base_id": "..."}   →    Creates CoW fork
                                  ←    {"connstring": <CLONE URL>}

Hand CLONE URL to agent          →    Agent: psql <CLONE URL>
                                       Agent reads, writes, creates tables
                                       Source is never touched

DELETE /clones/xxx               →    Clone destroyed
```

## How It Works

1. **Connect** — FDW proxy to source. Available instantly, zero data transfer.
2. **Stream** — Background worker copies tables locally via parallel COPY.
3. **Sync** — Once local, logical replication keeps data in real-time sync. Push-based, millisecond latency.
4. **Clone** — APFS (macOS) or ZFS/BTRFS/reflink (Linux) creates a Copy-on-Write fork. Sub-second.
5. **Isolate** — Each clone gets its own Postgres process, unique credentials, and private write layer.

## Deployment

### Quick Start (any server)

```bash
pip install mirrorbase
export MIRRORBASE_API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
export MIRRORBASE_HOST=mirrorbase.yourserver.com
mirrorbase serve
```

### Docker

```bash
docker compose up -d
```

### Production

```bash
export MIRRORBASE_API_KEY=<random-32-char-token>
export MIRRORBASE_ENCRYPTION_KEY=<random-32-char-key>
export MIRRORBASE_HOST=mirrorbase.yourserver.com
export MIRRORBASE_TLS_CERT=/certs/fullchain.pem
export MIRRORBASE_TLS_KEY=/certs/privkey.pem
export MIRRORBASE_ALLOWED_ORIGINS=https://yourapp.com
docker compose -f deploy/docker-compose.prod.yml up -d
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MIRRORBASE_API_KEY` | Yes | API key for authentication |
| `MIRRORBASE_HOST` | Yes (prod) | Public hostname for clone URLs (default: `localhost`) |
| `MIRRORBASE_ENCRYPTION_KEY` | Recommended | Encrypts connection strings at rest |
| `MIRRORBASE_HOME` | No | Data directory (default: `~/.mirrorbase`) |
| `MIRRORBASE_PG_BIN` | No | Postgres binary path (auto-detected) |
| `MIRRORBASE_TLS_CERT` | Recommended | TLS certificate path |
| `MIRRORBASE_TLS_KEY` | Recommended | TLS private key path |
| `MIRRORBASE_ALLOWED_ORIGINS` | Recommended | CORS origin whitelist |
| `MIRRORBASE_RATE_LIMIT` | No | Requests per minute per IP (default: 120) |
| `MIRRORBASE_PORT_START` | No | Clone port range start (default: 5500) |
| `MIRRORBASE_PORT_END` | No | Clone port range end (default: 5999) |

## Security

- **Auth**: API key on every request (constant-time comparison, no timing attacks)
- **Clone credentials**: Each clone gets a unique user + random 24-char password
- **Encryption at rest**: Source connection strings encrypted on disk
- **Masked responses**: Passwords never returned in API responses
- **CORS**: Origin whitelist only (no wildcards)
- **Rate limiting**: Per-IP request throttling
- **TLS**: HTTPS support for API + clone connections
- **Headers**: nosniff, DENY framing, no-store cache

## Python Client

For calling MirrorBase from another service:

```python
from mirrorbase.client import MirrorBaseClient

client = MirrorBaseClient(
    url="https://mirrorbase.yourserver.com:8100",
    api_key="your-api-key",
)

# Connect a customer database
base = client.connect("postgresql://customer:pass@host/db")

# Create a clone for an agent
clone = client.clone(base["base_id"])
agent_db_url = clone["connstring"]
# → postgresql://clone_user:rAnDoMpAsS@mirrorbase.yourserver.com:6001/db

# Destroy when done
client.destroy(clone["clone_id"])
```

## Real-Time Sync

For millisecond-latency sync, enable logical replication on the source:

- **Neon**: Project Settings → Logical Replication → Enable
- **RDS**: Set `rds.logical_replication = 1` in parameter group
- **Supabase**: Already enabled by default
- **Self-hosted**: Set `wal_level = logical` in `postgresql.conf`

Without it, MirrorBase still works — data syncs during connection, clones are instant. Just no live updates from source.

## Demos

```bash
python demos/01_connect_and_clone.py "postgresql://..."
python demos/02_multiple_clones.py "postgresql://..."
python demos/03_run_queries.py "postgresql://..."
python demos/04_write_to_clone.py "postgresql://..."
python demos/05_benchmark.py "postgresql://..."
python demos/07_agent_sandbox.py "postgresql://..."
python demos/08_advanced_queries.py "postgresql://..."
```

## License

[Business Source License 1.1](LICENSE)

Free for non-commercial use. For commercial/enterprise use, contact enterprise@spawnlabs.ai.

Converts to Apache 2.0 after 4 years.
