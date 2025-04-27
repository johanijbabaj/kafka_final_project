import json
import argparse
import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from products_generator import generate_products

# Kafka и Schema Registry конфигурация
KAFKA_BROKER = "127.0.0.1:9094"
KAFKA_TOPIC = "products"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
SCHEMA_FILE = "shop_api/product_schema.avsc"  # Путь к файлу схемы

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Чтение Avro-схемы из файла
with open(SCHEMA_FILE, "r") as f:
    value_schema_str = f.read()

schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Создаем сериализатор Avro
avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

def send_products_to_kafka(products, broker, topic, avro_serializer):
    """
    Sends a list of products to Kafka topic using Avro and Schema Registry.
    """
    producer = Producer({'bootstrap.servers': broker})

    for product in products:
        # Сериализация данных с использованием AvroSerializer
        serialized_product = avro_serializer(
            product,
            SerializationContext(topic, MessageField.VALUE)
        )
        producer.produce(topic=topic, value=serialized_product)
        logger.info(f"Product {product['product_id']} sent to Kafka topic {topic}")

    producer.flush()

def send_product_from_file(file_path, broker, topic, avro_serializer):
    """
    Reads JSON from file and sends it to Kafka topic using Avro and Schema Registry.
    """
    try:
        with open(file_path, "r") as f:
            product = json.load(f)
        send_products_to_kafka([product], broker, topic, avro_serializer)
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
        send_product_from_file(args.file, KAFKA_BROKER, KAFKA_TOPIC, avro_serializer)
    elif args.generate:
        products = generate_products(args.generate)
        send_products_to_kafka(products, KAFKA_BROKER, KAFKA_TOPIC, avro_serializer)
