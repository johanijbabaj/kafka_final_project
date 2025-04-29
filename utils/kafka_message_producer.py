import json
import time

from kafka import KafkaProducer
from message_generator import MessageGenerator

producer = KafkaProducer(
    bootstrap_servers='kafka-00:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "messages"


generator = MessageGenerator(users_count=10, message_rate=5)

try:
    print("Starting message generation...")
    generator.start_generating(producer, topic)

    time.sleep(30)
finally:
    print("Stopping message generation...")
    generator.stop_generating()
    producer.close()