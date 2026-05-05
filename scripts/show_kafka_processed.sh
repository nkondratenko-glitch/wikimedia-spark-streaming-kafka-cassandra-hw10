#!/usr/bin/env bash
set -euo pipefail

docker exec -it hw10-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic processed \
  --from-beginning \
  --max-messages 10
