import faust
from config import APP_NAME, KAFKA_BROKER, STORE_URL, WEB_PORT
from avro_serializers import FaustAvroSerializer

products_serializer = FaustAvroSerializer('products-value')

app = faust.App(
    APP_NAME,
    broker=KAFKA_BROKER,
    web_port=WEB_PORT,
    store=STORE_URL
)
