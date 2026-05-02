import json
import random
import time
import os
from datetime import datetime, timezone
from confluent_kafka import Producer

# --- Config ---
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
SSL_CA = os.getenv("KAFKA_SSL_CA", "./certs/ca.pem")
SSL_CERT = os.getenv("KAFKA_SSL_CERT", "./certs/service.cert")
SSL_KEY = os.getenv("KAFKA_SSL_KEY", "./certs/service.key")

TOPIC = "raw_trades"
TICKERS = ["AAPL", "NVDA", "TSLA", "SPY", "AMD"]
INTERVAL_SECONDS = 30  # 1 msg per ticker per 30s = ~3,900/market-day
spike_counter = 0

# Realistic base prices per ticker
BASE_PRICES = {
    "AAPL": 190.0,
    "NVDA": 850.0,
    "TSLA": 175.0,
    "SPY": 520.0,
    "AMD": 165.0,
}

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SSL",
    "ssl.ca.location": SSL_CA,
    "ssl.certificate.location": SSL_CERT,
    "ssl.key.location": SSL_KEY,
})

def generate_trade(ticker):
    global spike_counter
    base = BASE_PRICES[ticker]
    price = round(base + random.uniform(-base * 0.005, base * 0.005), 2)
    
    # Every 10th batch, spike NVDA volume 10x
    if ticker == "NVDA" and spike_counter % 10 == 0:
        size = random.randint(40000, 50000)  # 10x normal
    else:
        size = random.randint(100, 5000)
    
    return {
        "ticker": ticker,
        "price": price,
        "size": size,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "is_synthetic": True,
    }

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed for {msg.key()}: {err}")
    else:
        print(f"[OK] {msg.topic()} | {msg.value().decode('utf-8')}")

print("Starting synthetic producer... Press Ctrl+C to stop.")
while True:
    for ticker in TICKERS:
        trade = generate_trade(ticker)
        producer.produce(
            TOPIC,
            key=ticker,
            value=json.dumps(trade),
            callback=delivery_report,
        )
        producer.poll(0)
    spike_counter += 1       
    producer.flush()
    print(f"--- Batch sent at {datetime.now().strftime('%H:%M:%S')} | sleeping {INTERVAL_SECONDS}s ---")
    time.sleep(INTERVAL_SECONDS)
