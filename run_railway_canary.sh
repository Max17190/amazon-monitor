#!/bin/sh
# One-shot Railway canary. Secrets are never printed and auth is scrubbed on exit.
set -eu

cleanup() {
  rm -f auth.json
}
trap cleanup EXIT

if [ -n "${AUTH_JSON:-}" ]; then
  umask 077
  printf '%s' "$AUTH_JSON" > auth.json
  chmod 600 auth.json
fi

if [ ! -f auth.json ]; then
  echo "ERROR: auth.json missing and AUTH_JSON not set" >&2
  exit 1
fi

export LATENCY_REPS="${LATENCY_REPS:-15}"
export LATENCY_ASINS="${LATENCY_ASINS:-B00FLYWNYQ,B07FZ8S74R,B0DT7L98J1,B0DTJFSSZG,B08N5WRWNW,B0D1XD1ZV3,B0C33XXS56}"
export LATENCY_BATCH_ASINS="${LATENCY_BATCH_ASINS:-$LATENCY_ASINS}"
export LATENCY_SPACING="${LATENCY_SPACING:-4.0}"
export LATENCY_MODE="${LATENCY_MODE:-fast}"
export LATENCY_RETRY_429="${LATENCY_RETRY_429:-2}"
export TVSS_TIMEOUT="${TVSS_TIMEOUT:-20}"
export PROXY_MODE="${PROXY_MODE:-fallback}"
export RAILWAY_CANARY_MODE="${RAILWAY_CANARY_MODE:-latency}"

python hot_path_benchmark.py --iterations 10000

if [ "$RAILWAY_CANARY_MODE" = "durable-latency" ]; then
  python durable_latency_benchmark.py \
    --database-url "${DATABASE_URL:?DATABASE_URL is required}" \
    --iterations "${DURABLE_LATENCY_ITERATIONS:-100}" \
    --warmup-iterations "${DURABLE_LATENCY_WARMUP_ITERATIONS:-10}" \
    --confirm-database-writes
elif [ "$RAILWAY_CANARY_MODE" = "cadence" ]; then
  python cadence_canary.py \
    --confirm \
    --asins "${CANARY_ASINS:-$LATENCY_ASINS}" \
    --discovery-seconds "${CANARY_DISCOVERY_SECONDS:-60}" \
    --quiet-seconds "${CANARY_QUIET_SECONDS:-900}" \
    --validation-observations "${CANARY_VALIDATION_OBSERVATIONS:-120}"
elif [ "$RAILWAY_CANARY_MODE" = "regional" ]; then
  python regional_canary.py \
    --confirm \
    --asins "${CANARY_ASINS:-$LATENCY_ASINS}" \
    --interval "${CANARY_REGIONAL_INTERVAL:-5.0}" \
    --quiet-seconds "${CANARY_QUIET_SECONDS:-900}" \
    --observations "${CANARY_VALIDATION_OBSERVATIONS:-120}"
else
  python latency_e2e.py
fi
