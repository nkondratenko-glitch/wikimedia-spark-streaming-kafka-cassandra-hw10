#!/usr/bin/env bash
set -euo pipefail

docker compose up -d zookeeper kafka kafka-init
docker compose up -d cassandra
sleep 35
docker exec -i hw10-cassandra cqlsh < cassandra/schema.cql

docker compose up -d spark-master spark-worker
docker compose up -d --build generator

docker exec -d hw10-spark-master bash -lc '
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/bitnami/spark/jobs/stream_input_to_processed.py \
    > /tmp/stream_input_to_processed.log 2>&1
'

docker exec -d hw10-spark-master bash -lc '
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    /opt/bitnami/spark/jobs/stream_processed_to_cassandra.py \
    > /tmp/stream_processed_to_cassandra.log 2>&1
'

echo "All components started. Let the pipeline run for 3-5 minutes."
