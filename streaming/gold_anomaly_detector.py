import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, count, lit, when,
    current_timestamp, round as spark_round, least
)

GCS_BUCKET  = os.getenv("GCS_BUCKET")
GCP_CREDS   = "/opt/certs/marketpulse-494919-98defaa69bff.json"

SILVER_PATH     = f"gs://{GCS_BUCKET}/silver/trade_agg"
GOLD_PATH       = f"gs://{GCS_BUCKET}/gold/anomalies"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/gold_anomalies"

ROLLING_WINDOW  = 20   # number of past windows for baseline
VOL_SPIKE_Z     = 3.0  # z-score threshold
PRICE_DEV_PCT   = 0.02 # 2% intra-window price deviation
WASH_VOL_MULT   = 1.5  # 1.5x baseline volume
WASH_PRICE_PCT  = 0.001 # 0.1% price range for wash signal

spark = SparkSession.builder \
    .appName("MarketPulse-GoldAnomalyDetector") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def detect_anomalies(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # Read full Silver history for rolling baseline
    try:
        silver_history = spark.read.format("delta").load(SILVER_PATH)
    except Exception:
        print(f"[Batch {batch_id}] Silver not ready yet, skipping.")
        return

    # Compute rolling baseline per ticker (last 20 windows)
    baseline = silver_history \
        .groupBy("ticker") \
        .agg(
            avg("total_volume").alias("baseline_volume"),
            stddev("total_volume").alias("std_volume"),
            count("*").alias("window_count")
        )

    # Join current batch with baseline
    enriched = batch_df.join(baseline, on="ticker", how="left")

    anomalies = []

    # ── Signal 1: Volume Spike ───────────────────────────────────────────
    vol_spike = enriched.filter(
        (col("window_count") >= 5) &
        (col("std_volume") > 0) &
        ((col("total_volume") - col("baseline_volume")) / col("std_volume") > VOL_SPIKE_Z)
    ).withColumn("signal_type", lit("VOLUME_SPIKE")) \
     .withColumn("z_score", spark_round(
         (col("total_volume") - col("baseline_volume")) / col("std_volume"), 4)) \
     .withColumn("confidence", spark_round(
         least(col("z_score") / 5.0, lit(1.0)), 4)) \
     .withColumn("detected_at", current_timestamp())
    anomalies.append(vol_spike)

    # ── Signal 2: Price Deviation ────────────────────────────────────────
    price_dev = enriched.filter(
        (col("vwap") > 0) &
        ((col("price_high") - col("price_low")) / col("vwap") > PRICE_DEV_PCT)
    ).withColumn("signal_type", lit("PRICE_DEVIATION")) \
     .withColumn("z_score", lit(None).cast("double")) \
     .withColumn("confidence", spark_round(
         least(
             (col("price_high") - col("price_low")) / col("vwap") / lit(0.05),
             lit(1.0)
         ), 4)) \
     .withColumn("detected_at", current_timestamp())
    anomalies.append(price_dev)

    # ── Signal 3: Wash Signal ────────────────────────────────────────────
    wash = enriched.filter(
        (col("window_count") >= 5) &
        (col("total_volume") > col("baseline_volume") * WASH_VOL_MULT) &
        ((col("price_high") - col("price_low")) / col("vwap") < WASH_PRICE_PCT)
    ).withColumn("signal_type", lit("WASH_SIGNAL")) \
     .withColumn("z_score", lit(None).cast("double")) \
     .withColumn("confidence", lit(0.75)) \
     .withColumn("detected_at", current_timestamp())
    anomalies.append(wash)

    # ── Combine and write to Gold ────────────────────────────────────────
    gold_cols = [
        "ticker", "detected_at", "signal_type", "confidence",
        "window_start", "window_end", "vwap", "total_volume",
        "baseline_volume", "z_score"
    ]

    from functools import reduce
    from pyspark.sql import DataFrame

    non_empty = [df.select(gold_cols) for df in anomalies if df.count() > 0]

    if not non_empty:
        print(f"[Batch {batch_id}] No anomalies detected.")
        return

    combined = reduce(DataFrame.unionByName, non_empty)

    if combined.count() > 0:
        combined.write.format("delta") \
            .mode("append") \
            .save(GOLD_PATH)
        print(f"[Batch {batch_id}] Wrote {combined.count()} anomalies to Gold.")
    else:
        print(f"[Batch {batch_id}] No anomalies detected.")

# Read Silver as streaming source
silver_stream = spark.readStream \
    .format("delta") \
    .load(SILVER_PATH)

query = silver_stream.writeStream \
    .foreachBatch(detect_anomalies) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="60 seconds") \
    .start()

print(f"Gold anomaly detector running. Reading from {SILVER_PATH}")
query.awaitTermination()