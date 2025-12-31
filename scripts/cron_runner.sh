#!/bin/sh
set -eu

url="$1"
job="$2"

if curl --fail --retry 3 --retry-all-errors --retry-delay 5 -sS -X POST "$url" -w " status=%{http_code}\n" >>/proc/1/fd/1 2>>/proc/1/fd/2; then
  exit 0
fi

if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
  payload=$(printf '{"text":"Cron job failed: %s url=%s"}' "$job" "$url")
  curl -sS -X POST -H "Content-Type: application/json" -d "$payload" "$ALERT_WEBHOOK_URL" >>/proc/1/fd/1 2>>/proc/1/fd/2 || true
fi

exit 1
