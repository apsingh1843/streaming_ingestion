from kafka import KafkaProducer
import json
import time
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'user_events'

def generate_event():
    return {
        'user_id': fake.uuid4(),
        'event_type': random.choice(['click', 'view', 'purchase']),
        'product_id': random.randint(1, 100),
        'amount': round(random.uniform(10.0, 500.0), 2),
        'timestamp': fake.iso8601()
    }

if __name__ == "__main__":
    while True:
        event = generate_event()
        print(f"Sending: {event}")
        producer.send(TOPIC, value=event)
        time.sleep(1)