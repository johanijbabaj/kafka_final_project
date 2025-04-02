import json
import argparse
import logging
from kafka import KafkaProducer
from products_generator import generate_products

# Kafka configuration
KAFKA_BROKER = "127.0.0.1:9094"
KAFKA_TOPIC = "products"

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def send_products_to_kafka(products, broker, topic):
    """
    Sends a list of products to Kafka topic.
    """
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for product in products:
        producer.send(topic, value=product)
        logger.info(f"Product {product['product_id']} sent to Kafka topic {topic}")
    producer.flush()

def send_product_from_file(file_path, broker, topic):
    """
    Reads JSON from file and sends it to Kafka topic.
    """
    try:
        with open(file_path, "r") as f:
            product = json.load(f)
        send_products_to_kafka([product], broker, topic)
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file {file_path}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send product(s) to Kafka topic.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Path to the JSON file with product data.")
    group.add_argument("--generate", type=int, help="Number of random products to generate and send.")
    args = parser.parse_args()

    if args.file:
        send_product_from_file(args.file, KAFKA_BROKER, KAFKA_TOPIC)
    elif args.generate:
        products = generate_products(args.generate)
        send_products_to_kafka(products, KAFKA_BROKER, KAFKA_TOPIC)