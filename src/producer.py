"""
RabbitMQ producer
"""

import json
import os
from dotenv import load_dotenv
import pika

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')

CONNECTION = pika.BlockingConnection(pika.URLParameters(RABBITMQ_HOST))
CHANNEL = CONNECTION.channel()

# Declare the queue
CHANNEL.queue_declare(queue='sparkNotification', durable=True)


def publish(data):
    """
    Publish
    """
    print 'Publishing .......'

    CHANNEL.basic_publish(exchange='', routing_key='sparkNotification', body=json.dumps(data))
