MarketPulse — Real-Time Market Anomaly Detection Pipeline

Live Dashboard: marketpulsemydata.streamlit.app

A production-grade streaming data pipeline that ingests live equity trade events, processes them through a cloud-native medallion lakehouse, detects anomalies in real time, and serves results through a publicly deployed dashboard.

What It Does
MarketPulse monitors 5 equity instruments (AAPL, NVDA, TSLA, SPY, AMD) for three types of unusual trading activity:

Volume Spike — Z-score > 3.0 vs 20-window rolling baseline (statistically extreme volume)
Price Deviation — Intra-minute price swing > 2% of VWAP (volatility burst)
Wash Signal — High volume + near-zero price movement (suspicious circular trading pattern)

Events flow from a Kafka producer through Bronze → Silver → Gold Delta Lake layers on GCS, into BigQuery, through dbt transformations, and out to a live Streamlit dashboard — with sub-60 second end-to-end latency.

Architecture
[Synthetic Producer / Alpaca API]
         │  JSON trade events (ticker, price, size, timestamp)
         │  1 msg/ticker/30s → ~3,900 msgs/market-day
         ▼
[Aiven Kafka]  ←  managed cloud, SSL/TLS, key-based partitioning by ticker
  topic: raw_trades (1 partition)
         │
         ▼
[Spark Structured Streaming]  ←  Docker (bitnami/spark:3.5)
  ├── bronze_writer.py       → Delta Lake Bronze (raw ticks)
  ├── silver_aggregator.py   → Delta Lake Silver (1-min VWAP windows)
  └── gold_anomaly_detector.py → Delta Lake Gold (anomaly rows only)
         │
         ▼
[Delta Lake on GCS]  ←  Google Cloud Storage (always-free tier)
  gs://marketpulse-bronze-utkarsh/
  ├── bronze/trades/         ← raw Parquet + _delta_log
  ├── silver/trade_agg/      ← 1-min aggregations + _delta_log
  ├── gold/anomalies/        ← flagged events + _delta_log
  └── checkpoints/           ← Spark fault-tolerance checkpoints
         │
         ▼
[gcs_to_bq.py]  ←  Python loader (WRITE_TRUNCATE, autodetect schema)
         │
         ▼
[BigQuery]  ←  GCP serverless warehouse (always-free tier)
  dataset: marketpulse_gold
  ├── anomalies              ← raw Gold table
  ├── daily_anomaly_summary  ← dbt model
  ├── hourly_anomaly_rate    ← dbt model
  └── ticker_health          ← dbt model (freshness SLA)
         │
         ▼
[Streamlit Dashboard]  ←  deployed on Streamlit Cloud (public URL)
  Tab 1: Live Anomaly Feed (last 50 anomalies, color-coded by signal)
  Tab 2: Ticker Stats (7-day anomaly counts, bar chart, dominant signal)

Tech Stack
LayerTechnologyPurposeIngestionPython + confluent-kafkaSynthetic trade event producerMessage QueueAiven Kafka (free tier)Fault-tolerant event buffer, SSL authStream ProcessingPySpark 3.5 Structured StreamingWindowed aggregations, anomaly detectionContainer RuntimeDocker + Docker ComposePortable Spark environmentStorage FormatDelta Lake (delta-spark 3.2)ACID transactions, time travel, checkpointingData LakeGoogle Cloud StorageBronze/Silver/Gold medallion layersWarehouseBigQueryServerless SQL, dashboard-connectableTransformationdbt (dbt-bigquery 1.8)SQL models, not_null + uniqueness testsDashboardStreamlitLive anomaly feed, publicly deployedCredentialsGCP Service Account + st.secretsSecure auth for GCS, BigQuery, Streamlit

Pipeline Metrics
MetricValueTickers monitored5 (AAPL, NVDA, TSLA, SPY, AMD)Events per market day~3,900 (throttled for free tier)End-to-end latency< 60 seconds (event → Gold table)Tumbling window size1 minuteAnomaly detection signals3 (Volume Spike, Price Deviation, Wash Signal)Z-score threshold> 3.0 (0.3% false positive rate)Rolling baseline window20 windows per tickerdbt tests8 (not_null + uniqueness, all passing)GCS storage used~100MB (Bronze + Silver + Gold Delta files)BigQuery storage< 1MB (free tier, never charged)

Anomaly Detection Logic
Signal 1 — Volume Spike
z_score = (current_volume - mean_volume_20w) / std_volume_20w
flag if z_score > 3.0
confidence = min(z_score / 5.0, 1.0)
Catches: institutional moves, news-driven surges, potential manipulation
Signal 2 — Price Deviation
deviation = (price_high - price_low) / vwap
flag if deviation > 0.02  (2%)
confidence = min(deviation / 0.05, 1.0)
Catches: intra-minute volatility bursts, flash events, algorithmic swings
Signal 3 — Wash Signal
flag if total_volume > baseline_volume * 1.5
     AND (price_high - price_low) / vwap < 0.001  (0.1%)
confidence = 0.75  (fixed — binary pattern)
Catches: high volume + near-zero price movement = suspicious circular trading

Data Schemas
Bronze — raw_trades
FieldTypeNotestickerSTRINGAAPL, NVDA, TSLA, SPY, AMDpriceFLOATTrade pricesizeINTEGERShare volume at this pricetimestampTIMESTAMPEvent time (UTC)ingested_atTIMESTAMPProducer insertion timeis_syntheticBOOLEANTrue for non-market-hours data
Silver — trade_agg
FieldTypeNotestickerSTRINGwindow_startTIMESTAMP1-min tumbling window startwindow_endTIMESTAMP1-min tumbling window endvwapFLOATsum(price×size) / sum(size)total_volumeLONGSum of trade sizes in windowtrade_countINTEGERNumber of trades in windowprice_highFLOATMax price in windowprice_lowFLOATMin price in window
Gold — anomalies
FieldTypeNotestickerSTRINGdetected_atTIMESTAMPWhen Spark flagged itsignal_typeSTRINGVOLUME_SPIKE / PRICE_DEVIATION / WASH_SIGNALconfidenceFLOAT0.0–1.0 signal strengthwindow_startTIMESTAMPWindow that triggered the flagwindow_endTIMESTAMPvwapFLOATFair price at detection timetotal_volumeLONGActual volume that triggered flagbaseline_volumeFLOAT20-window rolling mean at detection timez_scoreFLOATStandard deviations above normal (Volume Spike only)

dbt Models
ModelMaterializationWhat It Computesdaily_anomaly_summarytableAnomaly counts by date, ticker, signal type + avg/max confidencehourly_anomaly_ratetableSame breakdown by hour — identifies most volatile trading hoursticker_healthtableOne row per ticker: 7-day count, last seen, dominant signal type
dbt tests: not_null and unique on key columns across all three models. All 8 tests passing.

Project Structure
marketpulse/
├── producers/
│   └── synthetic_producer.py      # Generates fake trade events 24/7
├── streaming/
│   ├── bronze_writer.py           # Kafka → Delta Bronze on GCS
│   ├── silver_aggregator.py       # Kafka → 1-min VWAP windows → Silver
│   ├── gold_anomaly_detector.py   # Silver Delta → anomaly detection → Gold
│   └── kafka_consumer_test.py     # Day 1 connectivity test
├── loaders/
│   └── gcs_to_bq.py              # Gold Parquet → BigQuery (WRITE_TRUNCATE)
├── dbt/
│   └── marketpulse/
│       └── models/
│           ├── daily_anomaly_summary.sql
│           ├── hourly_anomaly_rate.sql
│           ├── ticker_health.sql
│           ├── sources.yml
│           └── schema.yml
├── dashboard/
│   └── app.py                    # Streamlit two-tab dashboard
├── docker-compose.yml             # Spark container with 2GB memory
├── requirements.txt               # Dashboard dependencies
├── .env.example                   # Required environment variables
└── README.md

Local Setup
Prerequisites

Mac/Linux (tested on macOS with Apple Silicon and x86)
Python 3.11+
Docker Desktop
GCP account (free tier — credit card required but never charged)
Aiven account (free — GitHub login works)


1. Clone and configure environment

git clone https://github.com/utkarsh1610-s/marketpulse.git
cd marketpulse
python3 -m venv venv
source venv/bin/activate
pip install confluent-kafka google-cloud-bigquery google-cloud-storage pandas pyarrow db-dtypes streamlit

Copy .env.example to .env and fill in your credentials:
cp .env.example .env

Required variables:
KAFKA_BOOTSTRAP_SERVER=<your-aiven-bootstrap-server>
KAFKA_SSL_CA=./certs/ca.pem
KAFKA_SSL_CERT=./certs/service.cert
KAFKA_SSL_KEY=./certs/service.key
GOOGLE_APPLICATION_CREDENTIALS=./certs/<your-service-account>.json
GCS_BUCKET=<your-gcs-bucket>
BQ_DATASET=marketpulse_gold
GCP_PROJECT_ID=<your-gcp-project-id>

2. Download required JARs

mkdir jars
curl -L -o jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
curl -L -o jars/kafka-clients-3.4.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
curl -L -o jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar
curl -L -o jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
curl -L -o jars/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
curl -L -o jars/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
curl -L -o jars/gcs-connector-hadoop3-latest.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

3. Start the pipeline

Open 4 terminal tabs and run each:
Tab 1 — Synthetic Producer
source venv/bin/activate
export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
python3 producers/synthetic_producer.py

Tab 2 — Bronze Writer
docker compose up -d
export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
docker exec -it -e KAFKA_BOOTSTRAP_SERVER=$KAFKA_BOOTSTRAP_SERVER -e GCS_BUCKET=$GCS_BUCKET marketpulse-spark-1 \
  spark-submit --conf "spark.driver.memory=2g" --conf "spark.executor.memory=2g" \
  --jars /opt/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/jars/kafka-clients-3.4.0.jar,/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/jars/commons-pool2-2.11.1.jar,/opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/gcs-connector-hadoop3-latest.jar \
  /opt/streaming/bronze_writer.py

Tab 3 — Silver Aggregator
docker exec -it -e KAFKA_BOOTSTRAP_SERVER=$KAFKA_BOOTSTRAP_SERVER -e GCS_BUCKET=$GCS_BUCKET marketpulse-spark-1 \
  spark-submit --conf "spark.driver.memory=2g" --conf "spark.executor.memory=2g" \
  --jars /opt/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/jars/kafka-clients-3.4.0.jar,/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/jars/commons-pool2-2.11.1.jar,/opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/gcs-connector-hadoop3-latest.jar \
  /opt/streaming/silver_aggregator.py

Tab 4 — Gold Anomaly Detector
docker exec -it -e KAFKA_BOOTSTRAP_SERVER=$KAFKA_BOOTSTRAP_SERVER -e GCS_BUCKET=$GCS_BUCKET marketpulse-spark-1 \
  spark-submit --conf "spark.driver.memory=2g" --conf "spark.executor.memory=2g" \
  --jars /opt/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/jars/kafka-clients-3.4.0.jar,/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/jars/commons-pool2-2.11.1.jar,/opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/gcs-connector-hadoop3-latest.jar \
  /opt/streaming/gold_anomaly_detector.py

4. Load into BigQuery and run dbt

After the pipeline has run for at least 30 minutes:
python3 loaders/gcs_to_bq.py
cd dbt/marketpulse && dbt run && dbt test

5. Run the dashboard locally

cd marketpulse
streamlit run dashboard/app.py



Author
Utkarsh Saraogi
MS Computer Science, Northeastern University
LinkedIn | GitHub