FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    redis-server \
    redis-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /var/app
WORKDIR /var/app

RUN python -m venv /var/app/.venv

COPY packages/producer.py packages/health.py packages/submit.py packages/entrypoint.sh packages/requirements.txt packages/.env ./
RUN /var/app/.venv/bin/pip install -r requirements.txt

RUN systemctl enable redis-server || true

RUN chmod +x /var/app/entrypoint.sh

ENTRYPOINT ["/bin/bash", "/var/app/entrypoint.sh"]
