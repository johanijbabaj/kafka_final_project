# avro_serializers.py
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationContext, MessageField
import logging
from config import SCHEMA_REGISTRY_URL

logger = logging.getLogger(__name__)

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})


def get_schema_str(subject_name):
    try:
        schema_info = schema_registry_client.get_latest_version(subject_name)
        return schema_info.schema.schema_str
    except Exception as e:
        logger.error(f"Error fetching schema for '{subject_name}': {e}")
        return None


def register_schema_if_not_exists(subject: str, schema_str: str):
    try:
        schema_registry_client.get_latest_version(subject)
    except SchemaRegistryError as e:
        if e.http_status_code == 404 and e.error_code == 40401:
            logger.info(f'Trying to register schema {schema_str}')
            schema_obj = Schema(schema_str, schema_type="AVRO")
            schema_registry_client.register_schema(subject, schema_obj)
        else:
            raise

class FaustAvroSerializer:
    def __init__(self, subject_name, schema_source_subject=None):
        """
        :param subject_name: The subject (topic-value) this serializer will be used for.
        :param schema_source_subject: Optional. If provided, schema will be fetched from here instead.
        """
        self.subject_name = subject_name
        self.schema_source_subject = schema_source_subject or subject_name
        self._schema_str = None
        self._serializer = None
        self._deserializer = None

    @property
    def schema_str(self):
        if self._schema_str is None:
            self._schema_str = get_schema_str(self.schema_source_subject)
        return self._schema_str

    def dumps(self, obj, **kwargs):
        schema_str = self.schema_str
        if schema_str and self._serializer is None:
            register_schema_if_not_exists(self.subject_name, schema_str)
            self._serializer = AvroSerializer(schema_registry_client, schema_str=schema_str)
        if self._serializer:
            try:
                context = SerializationContext(self.subject_name, MessageField.VALUE)
                return self._serializer(obj, context)
            except Exception as e:
                logger.error(f"Error during serialization for '{self.subject_name}': {e}")
                return None
        else:
            logger.error(f"Could not serialize object for '{self.subject_name}' as schema is not available.")
            return None

    def loads(self, bytestr, **kwargs):
        # Data type check before deserialization
        if isinstance(bytestr, dict):  # If a dict is passed, it needs to be serialized to bytes
            logger.info("Serializing dict to bytes for deserialization.")
            bytestr = self.dumps(bytestr)  # Serialize the object into a byte string

        schema_str = self.schema_str
        if schema_str and self._deserializer is None:
            self._deserializer = AvroDeserializer(schema_registry_client, schema_str=schema_str)

        if self._deserializer:
            try:
                context = SerializationContext(self.subject_name, MessageField.VALUE)
                return self._deserializer(bytestr, context)
            except Exception as e:
                logger.error(f"Error during deserialization for '{self.subject_name}': {e}")
                return None
        else:
            logger.error(f"Could not deserialize bytes for '{self.subject_name}' as schema is not available.")
            return None