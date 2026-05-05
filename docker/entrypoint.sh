#!/bin/sh
set -e

# Run once at startup unless disabled
if [ "${RUN_ON_STARTUP}" != "false" ]; then
    python -m manager
fi

# If CRON_SCHEDULE is set and non-empty, install the cron job and hand off to crond
if [ -n "${CRON_SCHEDULE}" ] && [ "${CRON_SCHEDULE}" != "false" ]; then
    echo "${CRON_SCHEDULE} cd /app && python -m manager >> /proc/1/fd/1 2>&1" \
        | crontab -
    exec cron -f
fi
