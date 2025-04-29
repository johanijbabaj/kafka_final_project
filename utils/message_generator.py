import random
import string
import time
import threading

from dataclasses import dataclass


@dataclass
class Message:
    user_id: str
    recipient_id: str
    text: str


class MessageGenerator:
    """
    Generates random users and messages for testing the Kafka message processing system.
    Allows customization of generation intensity.
    """

    def __init__(self, users_count=10, message_rate=10):
        """
        Initialize the MessageGenerator with user count and message rate.

        :param users_count: Number of random users to generate.
        :param message_rate: Number of messages to generate per second.
        """
        self.blocked_users = {"user999", "user003"}
        self.censored_words = {"badword", "another_badword", "superbad_word"}
        self.users = [self._generate_random_user() for _ in range(users_count)]
        self.message_rate = message_rate
        self.running = False
        

    def _generate_random_user(self):
        """
        Generates a random user ID.

        :return: Random user ID as a string.
        """
        # 20% chance to pick a blocked user
        if random.random() < 0.2:
            return random.choice(list(self.blocked_users))
        return "".join(random.choices(string.ascii_letters + string.digits, k=8))

    def _generate_random_message(self):
        """
        Generates a random message with random content and sender/receiver IDs.

        :return: Dictionary representing a random message.
        """
        sender = random.choice(self.users)
        receiver = random.choice(self.users)
        while receiver == sender:
            receiver = random.choice(self.users)

        # 20% chance to include a censored word in the message
        if random.random() < 0.2:
            content = random.choice(list(self.censored_words))
        else:
            content = "Message: " + "".join(random.choices(string.ascii_letters + string.digits, k=20))

        message = {
            "user_id": sender,
            "recipient_id": receiver,
            "text": content,
        }
        return message

    def start_generating(self, kafka_producer, topic):
        """
        Starts generating messages and sending them to a Kafka topic.

        :param kafka_producer: Kafka producer instance to send messages.
        :param topic: Kafka topic name to send the messages to.
        """
        self.running = True

        def generate():
            while self.running:
                message = self._generate_random_message()
                print(f'{message}')
                kafka_producer.send(topic, value=message)
                kafka_producer.flush()
                time.sleep(1 / self.message_rate)

        thread = threading.Thread(target=generate)
        thread.daemon = True
        thread.start()

    def stop_generating(self):
        """
        Stops generating messages.
        """
        self.running = False
