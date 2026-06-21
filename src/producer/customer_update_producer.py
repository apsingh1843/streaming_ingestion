from kafka import KafkaProducer
import json
import time
from faker import Faker
import random
import uuid
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'customer_updates'

CUSTOMER_MASTER_DATA = {
    'CUST1001': {'customer_name': 'John Doe', 'email': 'john.doe@example.com'},
    'CUST1002': {'customer_name': 'Jane Smith', 'email': 'jane.smith@example.com'},
    'CUST1003': {'customer_name': 'Bob Johnson', 'email': 'bob.johnson@example.com'},
    'CUST1004': {'customer_name': 'Alice Williams', 'email': 'alice.williams@example.com'},
    'CUST1005': {'customer_name': 'Charlie Brown', 'email': 'charlie.brown@example.com'}
}

def generate_cust_update():
    customer_id = random.choice(list(CUSTOMER_MASTER_DATA.keys()))
    customer = CUSTOMER_MASTER_DATA[customer_id]
    return {
        'customer_id': customer_id,
        'customer_name': customer['customer_name'],
        'email': customer['email'],
        'phone': fake.phone_number(),
        'city': random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow']),
        'risk_rating': random.choice(['LOW', 'MEDIUM', 'HIGH']),
        'update_timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    while True:
        customer = generate_cust_update()
        producer.send(TOPIC, value=customer)
        print(f"Sent: {customer}")
        time.sleep(5)