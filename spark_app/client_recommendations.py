import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, MapType
import json

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ClientRecommendationsApp")

def log_count(df, epoch_id, label):
    count = df.count()
    logger.info(f"[{label}] Batch {epoch_id}: {count} records")

# Spark session
spark = SparkSession.builder \
    .appName("ClientRecommendationsApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schemas
product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("description", StringType()),
    StructField("price", StructType([
        StructField("amount", DoubleType()),
        StructField("currency", StringType())
    ])),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("stock", MapType(StringType(), StringType())),
    StructField("sku", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("images", ArrayType(MapType(StringType(), StringType()))),
    StructField("specifications", MapType(StringType(), StringType())),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("index", StringType()),
    StructField("store_id", StringType())
])

message_schema = StructType([
    StructField("client_id", StringType()),
    StructField("product_name", StringType()),
    StructField("results", ArrayType(product_schema))
])


# Recommendation UDF
def get_best_product(record):
    try:
        if record and record.results and len(record.results) > 0:
            first_product = record.results[0]
            return json.dumps({
                "client_id": record.client_id,
                "recommended_product_id": first_product.product_id
            })
        else:
            return None
    except Exception as e:
        logger.warning(f"Error in recommendation logic: {e}")
        return None


recommend_udf = udf(get_best_product, StringType())

# Step 1: Kafka read
kafka_source_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-0:9092") \
    .option("subscribe", "product_search_topic") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_source_df.writeStream \
    .foreachBatch(lambda df, epoch_id: log_count(df, epoch_id, "Step 1: Raw Kafka")) \
    .start()

# Step 2: Cast to string
messages_df = kafka_source_df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

messages_df.writeStream \
    .foreachBatch(lambda df, epoch_id: log_count(df, epoch_id, "Step 2: Casted values")) \
    .start()

# Step 3: Parse JSON
parsed_df = messages_df.withColumn("parsed_value", from_json(col("value"), message_schema))

parsed_df.writeStream \
    .foreachBatch(lambda df, epoch_id: log_count(df, epoch_id, "Step 3: Parsed JSON")) \
    .start()

parsed_df.writeStream \
    .foreachBatch(lambda df, epoch_id: (
        log_count(df, epoch_id, "Step 3.1: Parsed JSON sample 1 line"),
        df.select("parsed_value").show(1, truncate=False)
    )) \
    .start()

# Step 4: Filter valid
valid_df = parsed_df.filter(col("parsed_value").isNotNull())

valid_df.writeStream \
    .foreachBatch(lambda df, epoch_id: log_count(df, epoch_id, "Step 4: Valid JSON")) \
    .start()

# Step 5: Recommendation
recommendations_df = valid_df.withColumn("recommendation", recommend_udf(col("parsed_value")))

recommendations_df.writeStream \
    .foreachBatch(lambda df, epoch_id: log_count(df, epoch_id, "Step 5: Recommendations")) \
    .start()

# Step 6: Filter non-null recommendations
final_df = recommendations_df.filter(col("recommendation").isNotNull())

final_df.writeStream \
    .foreachBatch(lambda df, epoch_id: log_count(df, epoch_id, "Step 6: Filtered recommendations")) \
    .start()
with_client_id_df = final_df.withColumn("client_id", col("parsed_value.client_id"))

# Step 7: Write to Kafka
output_df = with_client_id_df.selectExpr(
    "CAST(client_id AS STRING) as key",
    "CAST(recommendation AS STRING) as value"
)

query_kafka_sink = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-0:9092") \
    .option("topic", "client_recommendations") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/client_recommendations") \
    .outputMode("append") \
    .start()

query_kafka_sink.awaitTermination()
