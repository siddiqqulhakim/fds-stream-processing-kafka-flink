from kafka import KafkaConsumer
import psycopg2, json, time, logging, sys

logging.basicConfig(level=logging.INFO)

print("🚀 Alert service starting...", flush=True)

# Retry connecting to Kafka
for i in range(10):
    try:
        consumer = KafkaConsumer(
            "fraud-alerts",
            bootstrap_servers="kafka:9092",
            group_id="alert-service",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        break
    except Exception as e:
        print(f"Kafka not ready, retrying... ({i+1}/10)", flush=True)
        time.sleep(3)
else:
    print("❌ Failed to connect to Kafka after 10 attempts", flush=True)
    sys.exit(1)

print("✅ Kafka consumer created, waiting for messages...", flush=True)

conn = psycopg2.connect(
    host="postgres",
    dbname="fraud",
    user="fraud",
    password="fraud"
)
cur = conn.cursor()

print("✅ Connected to PostgreSQL", flush=True)

for msg in consumer:
    alert = msg.value

    # print("🔥 RAW MESSAGE:", msg, flush=True)

    cur.execute(
        "INSERT INTO fraud_alerts (user_id, reason) VALUES (%s, %s)",
        (alert["user_id"], alert["reason"])
    )
    conn.commit()

    print("🚨 FRAUD ALERT:", alert, flush=True)
