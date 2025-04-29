from kafka import KafkaProducer
import logging

# Включаем логирование для отладки
logging.basicConfig(level=logging.DEBUG)

try:
    producer = KafkaProducer(bootstrap_servers='0.0.0.0:9094')
    topic = 'test_test'
    message = 'Hello, Kafka!'
    
    # Отправка сообщения в topic 'test'
    producer.send(topic, value=message.encode('utf-8'))
    
    producer.flush()
    producer.close()
    print(f"Message '{message}' sent to topic '{topic}'")
except Exception as e:
    print(f"Error: {e}")
