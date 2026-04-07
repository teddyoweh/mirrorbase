FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY mirrorbase/ mirrorbase/
RUN python3 -m venv /app/.venv \
    && /app/.venv/bin/pip install --no-cache-dir .

ENV MIRRORBASE_HOME=/data/mirrorbase
ENV MIRRORBASE_PG_BIN=/usr/lib/postgresql/17/bin
ENV MIRRORBASE_PORT_START=6000
ENV MIRRORBASE_PORT_END=6499
ENV PATH="/app/.venv/bin:$PATH"

# Security — these MUST be set at runtime, not build time
# MIRRORBASE_API_KEY=<random 32+ char token>
# MIRRORBASE_ENCRYPTION_KEY=<random 32+ char key>
# MIRRORBASE_ALLOWED_ORIGINS=https://spawnlabs.ai
# MIRRORBASE_TLS_CERT=/certs/cert.pem
# MIRRORBASE_TLS_KEY=/certs/key.pem

VOLUME /data/mirrorbase

EXPOSE 8100

CMD ["python3", "-m", "mirrorbase.server"]
