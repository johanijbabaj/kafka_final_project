FROM bitnami/spark:3.5.4

WORKDIR /app

COPY spark_app/kafka_to_hdfs.py .

RUN pip install confluent-kafka

CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4", "kafka_to_hdfs.py"]