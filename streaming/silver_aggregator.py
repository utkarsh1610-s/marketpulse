import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as spark_sum,
    count, max as spark_max, min as spark_min,
    round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType, TimestampType
)

GCS_BUCKET = os.getenv("GCS_BUCKET")
SSL_CA   = "/opt/certs/ca.pem"
SSL_CERT = "/opt/certs/service.cert"
SSL_KEY  = "/opt/certs/service.key"
GCP_CREDS = "/opt/certs/marketpulse-494919-98defaa69bff.json"

BRONZE_PATH    = f"gs://{GCS_BUCKET}/bronze/trades"
SILVER_PATH    = f"gs://{GCS_BUCKET}/silver/trade_agg"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/silver_trade_agg"

trade_schema = StructType([
    StructField("ticker",       StringType()),
    StructField("price",        DoubleType()),
    StructField("size",         IntegerType()),
    StructField("timestamp",    StringType()),
    StructField("ingested_at",  StringType()),
    StructField("is_synthetic", BooleanType()),
])

spark = SparkSession.builder \
    .appName("MarketPulse-SilverAggregator") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka directly (same source as bronze)
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", "raw_trades") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", open(SSL_CA).read()) \
    .option("kafka.ssl.keystore.certificate.chain", open(SSL_CERT).read()) \
    .option("kafka.ssl.keystore.key", open(SSL_KEY).read()) \
    .load()

trades_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), trade_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# Apply 1-minute tumbling window with 10-min watermark
agg_df = trades_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("ticker")
    ) \
    .agg(
        spark_round(
            spark_sum(col("price") * col("size")) / spark_sum(col("size")), 4
        ).alias("vwap"),
        spark_sum("size").alias("total_volume"),
        count("*").alias("trade_count"),
        spark_max("price").alias("price_high"),
        spark_min("price").alias("price_low"),
    ) \
    .select(
        col("ticker"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("vwap"),
        col("total_volume"),
        col("trade_count"),
        col("price_high"),
        col("price_low"),
    )

query = agg_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start(SILVER_PATH)

print(f"Writing Silver layer to {SILVER_PATH}")
query.awaitTermination()