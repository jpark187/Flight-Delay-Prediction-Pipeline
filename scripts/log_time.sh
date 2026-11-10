#!/usr/bin/env bash
# log_time.sh ACTION RESOURCE -> appends a UTC timestamp line to timestamps.txt
# e.g.  scripts/log_time.sh CREATE vm/flights-loader
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)  $1  $2" >> "$(dirname "$0")/../timestamps.txt"
