#!/usr/bin/env bash
set -euo pipefail

MIN="${MIN_COVERAGE:-25}"

go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic -timeout=180s
total=$(go tool cover -func=coverage.out | awk '/^total:/{print $3}' | tr -d '%')
echo "total coverage: ${total}% (min ${MIN}%)"
awk -v t="$total" -v m="$MIN" 'BEGIN{exit !(t+0 >= m+0)}'

