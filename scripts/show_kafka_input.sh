#!/usr/bin/env bash
set -euo pipefail

docker exec -it hw10-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic input \
  --from-beginning \
  --max-messages 5
