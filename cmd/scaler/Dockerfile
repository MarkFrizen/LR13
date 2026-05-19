# ──────────────────────────────────────────────────────────────────────
# scaler — автоматическое масштабирование агентов
# ──────────────────────────────────────────────────────────────────────
# Многостадийная сборка: копируем docker + compose CLI из официального
# Docker-образа, рантайм — python:3.12-slim.
# ──────────────────────────────────────────────────────────────────────

FROM docker:latest AS docker-cli

FROM python:3.12-slim

# Копируем docker CLI и docker-compose plugin.
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/docker-compose \
    /usr/local/libexec/docker/cli-plugins/docker-compose

# Сертификаты для HTTPS (NATS/TLS).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cmd/scaler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scaler.py .

CMD ["python", "scaler.py"]
