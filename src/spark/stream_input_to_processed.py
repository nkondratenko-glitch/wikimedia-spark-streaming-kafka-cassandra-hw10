#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "input"
PROCESSED_TOPIC = "processed"

ALLOWED_DOMAINS = [
    "en.wikipedia.org",
    "www.wikidata.org",
    "commons.wikimedia.org",
]

schema = StructType(
    [
        StructField("domain", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_is_bot", BooleanType(), True),
        StructField("dt", StringType(), True),
        StructField("page_title", StringType(), True),
    ]
)

spark = SparkSession.builder.appName("wikimedia-input-to-processed").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("event")
).select("event.*")

filtered = (
    parsed.filter(col("domain").isin(ALLOWED_DOMAINS))
    .filter(col("user_is_bot") == False)
    .filter(col("user_id").isNotNull())
    .filter(col("dt").isNotNull())
    .filter(col("page_title").isNotNull())
)

output = filtered.select(
    col("domain").cast("string"),
    col("user_id").cast("string").alias("user_id"),
    col("dt").alias("created_at"),
    col("page_title").cast("string"),
)

kafka_output = output.select(
    col("domain").alias("key"),
    to_json(struct("user_id", "domain", "created_at", "page_title")).alias("value"),
)

query = (
    kafka_output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", PROCESSED_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/wikimedia_processed")
    .outputMode("append")
    .start()
)

query.awaitTermination()
