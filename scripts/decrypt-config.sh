#!/usr/bin/env bash
# Decrypt config/config.tar.age into $RUNNER_TEMP/config and point the scanner at it.
set -euo pipefail
: "${SCANNER_AGE_KEY:?SCANNER_AGE_KEY secret is not set}"
umask 077
printf '%s\n' "$SCANNER_AGE_KEY" > "$RUNNER_TEMP/scanner.key"
mkdir -p "$RUNNER_TEMP/config"
age -d -i "$RUNNER_TEMP/scanner.key" config/config.tar.age | tar -xzf - -C "$RUNNER_TEMP/config"
echo "MRF_CONFIG_DIR=$RUNNER_TEMP/config" >> "$GITHUB_ENV"
