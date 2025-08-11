from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['apple', 'banana', 'orange', 'mango']

def generate_order():
    return {
        'order_id': random.randint(1000, 9999),
        'product': random.choice(products),
        'quantity': random.randint(1, 5)
    }

if __name__ == '__main__':
    while True:
        order = generate_order()
        print(f"Sending order: {order}")
        producer.send('orders', order)
        producer.flush()
        time.sleep(2)
