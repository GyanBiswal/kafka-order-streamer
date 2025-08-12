from kafka import KafkaConsumer
import json
import os

# Make sure 'data' folder exists before starting to consume
os.makedirs('data', exist_ok=True) # this ensures that the folder where you want to save order logs (data/orders_log.txt) is present before you start writing files.

consumer = KafkaConsumer(
    'orders', # Subscribes to the topic 'orders'
    bootstrap_servers='localhost:9092', # Connects to Kafka broker at localhost:9092.
    auto_offset_reset='earliest', # Starts reading from the earliest message if no committed offset exists.
    group_id='order-group', # Uses a consumer group named 'order-group' for coordination and load balancing.
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) # Deserializes incoming messages from JSON bytes to Python dict.
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
