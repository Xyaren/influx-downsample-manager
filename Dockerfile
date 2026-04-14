FROM python:3.14

RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ADD requirements.txt /app
RUN pip3 install -r ./requirements.txt
ADD manager /app/manager

# Default: every 6 hours. Override with CRON_SCHEDULE env var.
ENV CRON_SCHEDULE="0 */6 * * *"

COPY <<'ENTRYPOINT' /app/entrypoint.sh
#!/bin/sh
set -e

# Run once at startup
python3 -m manager

# If CRON_SCHEDULE is set and non-empty, install the cron job and hand off to crond
if [ -n "${CRON_SCHEDULE}" ] && [ "${CRON_SCHEDULE}" != "false" ]; then
    echo "${CRON_SCHEDULE} cd /app && python3 -m manager >> /proc/1/fd/1 2>&1" \
        | crontab -
    exec cron -f
fi
ENTRYPOINT
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
