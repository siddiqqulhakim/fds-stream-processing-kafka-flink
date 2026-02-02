from fastapi import FastAPI
from kafka import KafkaProducer
import psycopg2, json, time


app = FastAPI()

def create_producer():
    for i in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            print("Kafka not ready, retrying...")
            time.sleep(3)
    raise Exception("Kafka not available")

producer = create_producer()

conn = psycopg2.connect(
    host="postgres", dbname="fraud", user="fraud", password="fraud"
)
cur = conn.cursor()

@app.post("/transactions")
def create_transaction(user_id: str, amount: int):
    cur.execute(
        "INSERT INTO transactions (user_id, amount) VALUES (%s, %s)",
        (user_id, amount)
    )
    conn.commit()

    event = {
        "user_id": user_id,
        "amount": amount,
        "ts": int(time.time() * 1000)
    }

    producer.send("transactions", event)

    return {"status": "OK"}
