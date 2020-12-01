"""Produce emails by randomly sampling data set and sending to kafka topic."""
import os
import json
from time import sleep
from kafka import KafkaProducer
# import utils

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
EMAILS_TOPIC = os.environ.get("EMAILS_TOPIC")
EMAILS_BATCH_SIZE = os.environ.get("EMAILS_BATCH_SIZE")
SLEEP_TIME = 10


def get_random_email_batch():
    batch = [
        {
            "to": "exampleuser1@mail.com",
            "from": "exampleuser@mail.com",
            "subject": "this is a subject",
            "body": "this is email body",
        },
        {
            "to": "exampleuser2@mail.com",
            "from": "exampleuser@mail.com",
            "subject": "this is a subject",
            "body": "this is email body",
        },
        {
            "to": "exampleuser3@mail.com",
            "from": "exampleuser@mail.com",
            "subject": "this is a subject",
            "body": "this is email body",
        },
    ]
    return batch


def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda x: json.dumps(x).encode('ascii'),
    )

    # Nit: move this line into a intialization script - it can crash easily...
    # This is idempotent but just smells a bt
    # utils.download_spam_data()

    while True:
        email_batch = get_random_email_batch()
        producer.send(EMAILS_TOPIC, value=email_batch)
        sleep(SLEEP_TIME)


if __name__ == "__main__":
    run()

