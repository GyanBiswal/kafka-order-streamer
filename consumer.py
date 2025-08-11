from kafka import KafkaConsumer
import json
import os

# Make sure 'data' folder exists before starting to consume
os.makedirs('data', exist_ok=True)

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Waiting for orders...")
with open('data/orders_log.txt', 'a') as f:
    f.write('Test write\n')

for message in consumer:
    order = message.value
    print(f"Received order: {order}")

    # Append order as JSON string to orders_log.txt file
    with open('data/orders_log.txt', 'a') as f:
        f.write(json.dumps(order) + '\n')
