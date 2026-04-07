#!/bin/bash
# Use MirrorBase via the REST API.
# Start server first: MIRRORBASE_API_KEY=secret mirrorbase serve

API="http://localhost:8100"
KEY="secret"

# Connect
curl -s -X POST $API/connect \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d "{\"url\": \"$1\"}"
echo

# Clone
# curl -s -X POST $API/clone \
#   -H "Authorization: Bearer $KEY" \
#   -H "Content-Type: application/json" \
#   -d '{"base_id": "base-xxxxx"}'
