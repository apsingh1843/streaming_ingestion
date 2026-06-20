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

TOPIC = 'banking_transactions'

def generate_transaction():
    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': f'CUST{random.randint(1000, 9999)}',
        'account_id': f'ACC{random.randint(10000, 99999)}',
        'transaction_type': random.choice(['UPI', 'CARD', 'ATM', 'NETBANKING']),
        'merchant': random.choice(['Amazon', 'Flipkart', 'Snapdeal', 'Myntra', 'BigBasket', 'Zomato', 'Swiggy']),
        'amount': round(random.uniform(10, 200000), 2),
        'city': random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow']),
        'risk_rating': random.choice(['LOW', 'MEDIUM', 'HIGH']),
        'device_id': f'DVC{random.randint(100000, 999999)}',
        'channel': random.choice(['MOBILE', 'WEB', 'POS', 'ATM']),
        'event_timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    while True:
        transaction = generate_transaction()
        producer.send(TOPIC, value=transaction)
        print(f"Sent: {transaction}")
        time.sleep(1)