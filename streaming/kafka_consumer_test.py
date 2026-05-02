import os
from pyspark.sql import SparkSession

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
SSL_CA = "/opt/certs/ca.pem"
SSL_CERT = "/opt/certs/service.cert"
SSL_KEY = "/opt/certs/service.key"

spark = SparkSession.builder \
    .appName("MarketPulse-KafkaTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", "raw_trades") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", open(SSL_CA).read()) \
    .option("kafka.ssl.keystore.certificate.chain", open(SSL_CERT).read()) \
    .option("kafka.ssl.keystore.key", open(SSL_KEY).read()) \
    .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
