import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka и HDFS конфигурация
KAFKA_BOOTSTRAP_SERVERS = "kafka-mirror-0:19092"
KAFKA_INPUT_TOPIC = "source.filtered_products"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/products_data"

# Spark сессия
logger.info("Starting Spark session...")
spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()
logger.info("Spark session started.")

# Схема для сообщений Kafka
schema = StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("price", StructType([
        StructField("amount", FloatType()),
        StructField("currency", StringType())
    ])),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("tags", ArrayType(StringType()))
])

# Чтение данных из Kafka
logger.info(f"Reading data from Kafka topic: {KAFKA_INPUT_TOPIC}")
try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_INPUT_TOPIC) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    logger.info("Successfully read data from Kafka.")
except Exception as e:
    logger.error(f"Error reading data from Kafka: {e}")
    exit(1)

# Запись данных в HDFS
logger.info(f"Writing data to HDFS path: {HDFS_OUTPUT_PATH}")
try:
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .option("path", HDFS_OUTPUT_PATH) \
        .start()
    logger.info("Streaming query started.")
    query.awaitTermination()
except Exception as e:
    logger.error(f"Error writing data to HDFS: {e}")
    exit(1)

logger.info("Streaming job finished.")