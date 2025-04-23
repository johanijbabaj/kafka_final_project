# topics.py
from app import app
from avro_serializers import FaustAvroSerializer
from config import PRODUCT_TOPIC, FILTERED_PRODUCT_TOPIC, BAN_RULES_TOPIC
from models import BanRule
import faust

# Подтягиваем только одну основную схему
products_serializer = FaustAvroSerializer('products-value')

# filtered_products будет использовать схему от products
filtered_products_serializer = FaustAvroSerializer('filtered_products-value', schema_source_subject='products-value')

# Faust schemas
products_schema = faust.Schema(
    key_type=bytes,
    value_type=bytes,
    value_serializer=products_serializer,
)

filtered_products_schema = faust.Schema(
    key_type=bytes,
    value_type=bytes,
    value_serializer=filtered_products_serializer,
)

# topics
products_topic = app.topic(PRODUCT_TOPIC, schema=products_schema)
app.logger.info(f"Defined topic '{PRODUCT_TOPIC}' with Avro schema using custom serializer.")

filtered_products_topic = app.topic(FILTERED_PRODUCT_TOPIC, schema=filtered_products_schema)
app.logger.info(f"Defined topic '{FILTERED_PRODUCT_TOPIC}' using schema from 'products-value'.")

ban_rules_topic = app.topic(BAN_RULES_TOPIC, value_type=BanRule)
app.logger.info(f"Defined topic '{BAN_RULES_TOPIC}' expecting JSON dicts.")
