python simulator.py
pip install kafka-python
python simulator.py


import random
import time
from kafka import KafkaProducer
import json

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate fake accounts
accounts = [f"ACC{i}" for i in range(1, 51)]

def generate_transaction():
    sender = random.choice(accounts)
    receiver = random.choice(accounts)
    while receiver == sender:  # avoid self-transfers
        receiver = random.choice(accounts)
    amount = random.choice([9900, 10000, 5000, 7500])  # mimic laundering
    return {
        "sender": sender,
        "receiver": receiver,
        "amount": amount,
        "timestamp": time.time()
    }

# Stream transactions continuously
while True:
    txn = generate_transaction()
    producer.send('transactions', txn)
    print(f"Sent: {txn}")
    time.sleep(1)  # 1 transaction per second

