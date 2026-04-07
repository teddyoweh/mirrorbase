#!/bin/bash
# Production deployment setup for mirrorbase.spawnlabs.ai
#
# Run on a fresh server (GCE/EC2/Hetzner):
#   curl -sSL https://raw.githubusercontent.com/teddyoweh/mirrorbase/main/deploy/setup.sh | bash

set -e

echo "=== MirrorBase Production Setup ==="

# Generate secrets
export MIRRORBASE_API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
export MIRRORBASE_ENCRYPTION_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

echo ""
echo "Generated secrets (save these):"
echo "  MIRRORBASE_API_KEY=$MIRRORBASE_API_KEY"
echo "  MIRRORBASE_ENCRYPTION_KEY=$MIRRORBASE_ENCRYPTION_KEY"
echo ""

# Write .env
cat > .env << EOF
MIRRORBASE_API_KEY=$MIRRORBASE_API_KEY
MIRRORBASE_ENCRYPTION_KEY=$MIRRORBASE_ENCRYPTION_KEY
EOF

echo "Secrets written to .env"
echo ""
echo "Next steps:"
echo "  1. Place TLS certs in ./certs/fullchain.pem and ./certs/privkey.pem"
echo "  2. docker compose -f docker-compose.prod.yml up -d"
echo "  3. Test: curl -H 'Authorization: Bearer $MIRRORBASE_API_KEY' https://mirrorbase.spawnlabs.ai/health"
echo ""
echo "Your Spawn backend uses this API key to connect:"
echo ""
echo "  from mirrorbase.client import MirrorBaseClient"
echo "  client = MirrorBaseClient('https://mirrorbase.spawnlabs.ai', '$MIRRORBASE_API_KEY')"
