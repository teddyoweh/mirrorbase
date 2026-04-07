# Production Deployment

## What You Need

- A server (GCE, EC2, Hetzner, DigitalOcean — anything with a public IP)
- PostgreSQL 17 installed on that server
- A domain pointing to it (e.g. `mirrorbase.yourcompany.com`)
- TLS certificate (Let's Encrypt works fine)

## Step 1: Install MirrorBase

SSH into your server:

```bash
pip install mirrorbase
```

Or with Docker:

```bash
git clone https://github.com/teddyoweh/mirrorbase
cd mirrorbase
docker compose -f deploy/docker-compose.prod.yml up -d
```

## Step 2: Generate Secrets

```bash
# API key — your backend uses this to authenticate
export MIRRORBASE_API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

# Encryption key — encrypts customer connection strings on disk
export MIRRORBASE_ENCRYPTION_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

echo "Save these somewhere safe:"
echo "  MIRRORBASE_API_KEY=$MIRRORBASE_API_KEY"
echo "  MIRRORBASE_ENCRYPTION_KEY=$MIRRORBASE_ENCRYPTION_KEY"
```

## Step 3: Configure

```bash
# Your server's public hostname — this goes into clone connection strings
export MIRRORBASE_HOST=mirrorbase.yourcompany.com

# TLS (recommended)
export MIRRORBASE_TLS_CERT=/etc/letsencrypt/live/mirrorbase.yourcompany.com/fullchain.pem
export MIRRORBASE_TLS_KEY=/etc/letsencrypt/live/mirrorbase.yourcompany.com/privkey.pem

# CORS — which origins can call the API
export MIRRORBASE_ALLOWED_ORIGINS=https://yourapp.com
```

## Step 4: Run

```bash
mirrorbase serve
```

## Step 5: Test

```bash
# Health check
curl https://mirrorbase.yourcompany.com:8100/health

# Connect a database
curl -X POST https://mirrorbase.yourcompany.com:8100/connect \
  -H "Authorization: Bearer $MIRRORBASE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"url": "postgresql://user:pass@some-host/somedb"}'

# Clone it
curl -X POST https://mirrorbase.yourcompany.com:8100/clone \
  -H "Authorization: Bearer $MIRRORBASE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"base_id": "base-abc123"}'
```

The clone response gives you a Postgres URL:

```json
{
  "clone_id": "clone-xyz",
  "connstring": "postgresql://clone_xyz:rAnDoMpAsS@mirrorbase.yourcompany.com:6001/somedb",
  "elapsed": 0.6
}
```

## Step 6: Integrate With Your Backend

```python
from mirrorbase.client import MirrorBaseClient

client = MirrorBaseClient(
    url="https://mirrorbase.yourcompany.com:8100",
    api_key="YOUR_API_KEY",
)

# Customer onboards — connect their DB (once)
base = client.connect("postgresql://customer:pass@their-host/their_db")
# Save base["base_id"] in your database

# Agent needs a sandbox — clone (every time)
clone = client.clone(base["base_id"])
agent_db_url = clone["connstring"]
# Hand agent_db_url to the agent

# Agent done — destroy (every time)
client.destroy(clone["clone_id"])
```

## Firewall

Open these ports on your server:

| Port | Purpose |
|------|---------|
| 8100 | MirrorBase API (HTTP/HTTPS) |
| 6000-6099 | Clone Postgres connections |

Lock both down to only IPs that need access.

## Running as a systemd Service

```bash
cat > /etc/systemd/system/mirrorbase.service << 'EOF'
[Unit]
Description=MirrorBase
After=postgresql.service

[Service]
ExecStart=/usr/local/bin/mirrorbase serve
Environment=MIRRORBASE_API_KEY=your-key
Environment=MIRRORBASE_ENCRYPTION_KEY=your-key
Environment=MIRRORBASE_HOST=mirrorbase.yourcompany.com
Restart=always

[Install]
WantedBy=multi-user.target
EOF

systemctl enable mirrorbase
systemctl start mirrorbase
```

## All Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MIRRORBASE_API_KEY` | Yes | — | API authentication key |
| `MIRRORBASE_HOST` | Yes (prod) | `localhost` | Public hostname for clone URLs |
| `MIRRORBASE_ENCRYPTION_KEY` | Recommended | — | Encrypts credentials on disk |
| `MIRRORBASE_TLS_CERT` | Recommended | — | TLS cert path |
| `MIRRORBASE_TLS_KEY` | Recommended | — | TLS key path |
| `MIRRORBASE_ALLOWED_ORIGINS` | Recommended | — | CORS whitelist |
| `MIRRORBASE_HOME` | No | `~/.mirrorbase` | Data directory |
| `MIRRORBASE_PG_BIN` | No | auto-detect | Postgres binaries path |
| `MIRRORBASE_PORT_START` | No | `5500` | Clone port range start |
| `MIRRORBASE_PORT_END` | No | `5999` | Clone port range end |
| `MIRRORBASE_RATE_LIMIT` | No | `120` | Requests/min per IP |
