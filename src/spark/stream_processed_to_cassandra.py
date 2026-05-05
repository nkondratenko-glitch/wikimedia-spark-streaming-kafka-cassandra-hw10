#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:9092"
PROCESSED_TOPIC = "processed"

schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("page_title", StringType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("wikimedia-processed-to-cassandra")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", PROCESSED_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("event")
).select("event.*")

records = parsed.select(
    col("user_id"),
    col("domain"),
    to_timestamp(col("created_at")).alias("created_at"),
    col("page_title"),
).filter(col("created_at").isNotNull())


def write_batch_to_cassandra(batch_df, batch_id: int) -> None:
    count = batch_df.count()

    if count == 0:
        return

    print(f"Writing batch {batch_id} with {count} rows to Cassandra", flush=True)

    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="page_creations", keyspace="wikimedia")
        .save()
    )


query = (
    records.writeStream.foreachBatch(write_batch_to_cassandra)
    .option("checkpointLocation", "/tmp/checkpoints/wikimedia_cassandra")
    .outputMode("append")
    .start()
)

query.awaitTermination()
