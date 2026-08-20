#!/usr/bin/env bash
# Waits until the Kafka service container answers a metadata request. The image
# ships no health check, and an open port is not the same as a broker able to
# serve metadata, so ask it for the topic list instead.
set -euo pipefail

container="${1:?usage: wait-for-kafka.sh <container-id>}"

for attempt in $(seq 1 30); do
  if docker exec "$container" \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka answered a metadata request after ${attempt} attempt(s)."
    exit 0
  fi
  sleep 5
done

echo "Kafka did not become ready in time." >&2
docker logs "$container" 2>&1 | tail -50 >&2
exit 1
