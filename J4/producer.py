from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
topic = os.getenv("TOPIC")

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
for _ in range(100):
    producer.send(topic, b'some_message_bytes')
    print("Message sent")
producer.flush()