#!/bin/bash
set -eu
REST="${FLINK_REST:-jobmanager:8081}"
JAR="${JOB_JAR:-/opt/flink/usrlib/network-incidents.jar}"

ready=0
for _ in $(seq 1 40); do
  if /opt/flink/bin/flink list -m "$REST" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 3
done
if [ "$ready" -ne 1 ]; then
  echo "Flink REST at ${REST} did not become ready" >&2
  exit 1
fi

/opt/flink/bin/flink list -m "$REST" -r | while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*: \([0-9a-f]\{32\}\) :.*/\1/p')
  if [ -n "$id" ]; then
    /opt/flink/bin/flink cancel -m "$REST" "$id" || true
  fi
done

/opt/flink/bin/flink run -d -m "$REST" "$JAR"
echo "Submitted network-incidents-job"
