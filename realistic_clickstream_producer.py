import json
import random
import time
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Edge", "Firefox"]
TRAFFIC_SOURCE = ["organic", "google_ads", "email", "direct"]

PRODUCT_CATEGORIES = [
    "electronics",
    "books",
    "fashion",
    "home",
    "sports"
]


# ----------------------------
# EVENT BUILDER
# ----------------------------
def build_event(user_id, session_id, event_type, page, product=None):

    price = round(random.uniform(20, 800), 2)
    quantity = random.randint(1, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat(),
        "user_id": user_id,
        "session_id": session_id,

        "event_type": event_type,
        "page": page,

        "product_id": product or str(uuid.uuid4()),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "price": price,
        "quantity": quantity,
        "total_value": round(price * quantity, 2),

        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "traffic_source": random.choice(TRAFFIC_SOURCE),

        "country": fake.country(),
        "city": fake.city()
    }


# ----------------------------
# USER SESSION SIMULATION
# ----------------------------
def simulate_user_session():

    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    events = []

    # 1️⃣ usuário entra
    events.append(build_event(user_id, session_id, "page_view", "home"))

    # 2️⃣ navega categorias
    for _ in range(random.randint(1, 3)):
        events.append(build_event(user_id, session_id, "page_view", "category_page"))

    # 3️⃣ vê produto
    product_id = str(uuid.uuid4())
    events.append(build_event(user_id, session_id, "product_click", "product_page", product_id))

    # probabilidade de adicionar ao carrinho
    if random.random() < 0.7:
        events.append(build_event(user_id, session_id, "add_to_cart", "cart", product_id))

        # probabilidade de iniciar checkout
        if random.random() < 0.6:
            events.append(build_event(user_id, session_id, "checkout_start", "checkout", product_id))

            # probabilidade de compra
            if random.random() < 0.5:
                events.append(build_event(user_id, session_id, "purchase", "checkout", product_id))
            else:
                events.append(build_event(user_id, session_id, "cart_abandon", "cart", product_id))
        else:
            events.append(build_event(user_id, session_id, "cart_abandon", "cart", product_id))

    return events


# ----------------------------
# STREAM LOOP
# ----------------------------
def stream_clickstream():

    print("Starting realistic clickstream simulation...")

    while True:

        session_events = simulate_user_session()

        for event in session_events:
            print(json.dumps(event))
            time.sleep(random.uniform(0.3, 1.2))


if __name__ == "__main__":
    stream_clickstream()
