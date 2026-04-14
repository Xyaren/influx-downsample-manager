FROM python:3.14-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /install /usr/local
COPY manager /app/manager
RUN python -m compileall -b /app/manager \
    && find /app/manager -name "*.py" -delete \
    && find /app/manager -type d -name "__pycache__" -exec rm -rf {} +

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default: every 6 hours. Override with CRON_SCHEDULE env var.
ENV CRON_SCHEDULE="0 */6 * * *"
ENV RUN_ON_STARTUP="true"

ENTRYPOINT ["/app/entrypoint.sh"]
