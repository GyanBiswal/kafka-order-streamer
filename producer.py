from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    # Creates a Kafka producer client connected to Kafka broker running on localhost:9092.
    bootstrap_servers='localhost:9092',
    # value_serializer is a function that converts the Python dictionary order to a JSON string and encodes it to bytes because Kafka messages are sent as bytes.
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
        producer.send('orders', order) # orders is the topic, order is message payload
        producer.flush() # Ensures the message is actually sent (not buffered)
        time.sleep(2) # Waits 2 seconds before sending next order
