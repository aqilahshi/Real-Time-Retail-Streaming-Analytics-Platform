import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer


products = [
    {
        "product_id": "PROD0001",
        "product_name": "Wireless Mouse",
        "category": "Electronics",
        "unit_price": 89.90,
    },
    {
        "product_id": "PROD0002",
        "product_name": "Mechanical Keyboard",
        "category": "Electronics",
        "unit_price": 249.90,
    },
    {
        "product_id": "PROD0003",
        "product_name": "Running Shoes",
        "category": "Sports",
        "unit_price": 329.90,
    },
    {
        "product_id": "PROD0004",
        "product_name": "Perfume",
        "category": "Beauty",
        "unit_price": 199.90,
    },
    {
        "product_id": "PROD0005",
        "product_name": "Office Chair",
        "category": "Home",
        "unit_price": 699.90,
    },
]

cities = [
    "Kuala Lumpur",
    "Penang",
    "Johor Bahru",
    "Ipoh",
    "Shah Alam",
    "Melaka",
]


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)


def generate_purchase_event():
    product = random.choice(products)
    quantity = random.choices(
        [1, 2, 3, 4],
        weights=[65, 25, 8, 2],
        k=1,
    )[0]

    total_amount = round(product["unit_price"] * quantity, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": f"CUST{random.randint(1, 500):04d}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["unit_price"],
        "total_amount": total_amount,
        "city": random.choice(cities),
        "event_time": datetime.now().isoformat(timespec="seconds"),
    }


print("Sending purchase events to Kafka. Press Ctrl+C to stop.")

try:
    while True:
        event = generate_purchase_event()

        producer.send(
            topic="purchases",
            value=event,
            key=event["customer_id"].encode("utf-8"),
        )

        producer.flush()

        print(
            f"{event['event_time']} | "
            f"{event['customer_id']} | "
            f"{event['product_name']} | "
            f"Qty: {event['quantity']} | "
            f"RM{event['total_amount']:.2f}"
        )

        time.sleep(1)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.close()