import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
GCS_BUCKET = os.getenv("GCS_BUCKET")
SSL_CA = "/opt/certs/ca.pem"
SSL_CERT = "/opt/certs/service.cert"
SSL_KEY = "/opt/certs/service.key"
GCP_CREDS = "/opt/certs/marketpulse-494919-98defaa69bff.json"

BRONZE_PATH = f"gs://{GCS_BUCKET}/bronze/trades"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/bronze_trades_v2"

trade_schema = StructType([
    StructField("ticker", StringType()),
    StructField("price", DoubleType()),
    StructField("size", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("ingested_at", StringType()),
    StructField("is_synthetic", BooleanType()),
])

spark = SparkSession.builder \
    .appName("MarketPulse-BronzeWriter") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", "raw_trades") \
    .option("startingOffsets", "earliest") \
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
    .withColumn("processed_at", current_timestamp())

query = trades_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(BRONZE_PATH)

print(f"Writing Bronze layer to {BRONZE_PATH}")
query.awaitTermination()