"""Produce emails by randomly sampling data set and sending to kafka topic."""
import os
import json
from time import sleep
from kafka import KafkaProducer
import utils

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
EMAILS_TOPIC = os.environ.get("EMAILS_TOPIC")
EMAILS_PER_SECOND = float(os.environ.get("EMAILS_PER_SECOND"))
SLEEP_TIME = 1 / EMAILS_PER_SECOND


def get_random_email_batch():
    batch = [
        {
            "to": "exampleuser@mail.com",
            "from": "exampleuser@mail.com",
            "subject": "this is a subject",
            "body": "this is email body",
        },
        {
            "to": "exampleuser@mail.com",
            "from": "exampleuser@mail.com",
            "subject": "this is a subject",
            "body": "this is email body",
        },
        {
            "to": "exampleuser@mail.com",
            "from": "exampleuser@mail.com",
            "subject": "this is a subject",
            "body": "this is email body",
        },
    ]
    return batch


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda x: json.dumps(x).encode(),
    )

    # TODO: move this line into a intialization script
    # utils.download_spam_data()

    while True:
        email_batch = get_random_email_batch()
        producer.send(EMAILS_TOPIC, value=email_batch)
        sleep(SLEEP_TIME)
