from confluent_kafka import Producer
from faker import Faker
from datetime import datetime
import random
import json
import time
import logging

# ==============================
# CONFIG
# ==============================
BOOTSTRAP_SERVERS = "<event-hub-windows.net>:9093"
TOPIC = "ecommerce-events"
CONN_STR = "<connection-string>"

config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": CONN_STR,
    "client.id": "ecommerce-producer",
    "batch.num.messages": 1000,
    "linger.ms": 50,
    "acks": "all",
}

producer = Producer(config)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

fake = Faker()

# ==============================
# DOMAIN DATA
# ==============================
EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout",
    "payment"
]

PAYMENTS = ["credit_card", "paypal", "pix", "debit_card"]

PRODUCTS = [fake.uuid4() for _ in range(50)]
USERS = [fake.uuid4() for _ in range(30)]

# sessões ativas (simula usuários navegando)
ACTIVE_SESSIONS = [fake.uuid4() for _ in range(20)]


# ==============================
# EVENT GENERATOR
# ==============================
def generate_event():

    session_id = random.choice(ACTIVE_SESSIONS)
    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": fake.uuid4(),
        "event_time": datetime.utcnow().isoformat(),
        "event_type": event_type,
        "session_id": session_id,
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "price": round(random.uniform(20, 800), 2),
        "quantity": random.randint(1, 3),
        "payment_method": None,
        "status": None
    }

    # eventos finais
    if event_type == "payment":
        event["payment_method"] = random.choice(PAYMENTS)
        event["status"] = "confirmed"

    return event


# ==============================
# DELIVERY CALLBACK
# ==============================
def delivery_report(err, msg):
    if err:
        logging.error(f"FAILED: {err}")
    else:
        logging.info(
            f"Sent to {msg.topic()} partition={msg.partition()}"
        )


# ==============================
# PRODUCER LOOP
# ==============================
def send_events():

    while True:
        event = generate_event()

        producer.produce(
            TOPIC,
            key=event["session_id"].encode("utf-8"),  # ⭐ partition key
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(0.5)


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    try:
        send_events()
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    finally:
        producer.flush()
