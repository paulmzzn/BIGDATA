from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
load_dotenv()  # take environment variables from .env.
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
topic = os.getenv("TOPIC")

consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers)
for msg in consumer:
    print (msg)