FROM bitnami/spark:3.5.4

WORKDIR /app

RUN pip install requests

COPY spark_app/spark-avro_2.13-3.5.4.jar /opt/bitnami/spark/jars/spark-avro_2.13-3.5.4.jar

COPY spark_app/kafka_to_hdfs.py .
COPY spark_app/client_recommendations.py .

RUN pip install confluent-kafka

#CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4", "kafka_to_hdfs.py"]