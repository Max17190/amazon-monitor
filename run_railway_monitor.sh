#!/bin/sh
# Persistent Railway worker. Materialize the secret auth bundle inside the
# ephemeral container before replacing this shell with the monitor process.
set -eu

if [ -n "${AUTH_JSON:-}" ]; then
  umask 077
  printf '%s' "$AUTH_JSON" > auth.json
  chmod 600 auth.json
fi

if [ ! -f auth.json ]; then
  echo "ERROR: auth.json missing and AUTH_JSON not set" >&2
  exit 1
fi

exec python main.py
