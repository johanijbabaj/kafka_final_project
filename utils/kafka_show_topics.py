from kafka import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers="kafka-00:9092")

topics = admin_client.list_topics()
print("Available topics:", topics)