import logging
import sys
import time
import socket
from os import getenv
import threading
from collections import defaultdict

import pika
from transformers import pipeline

import movie_sanit_pb2
from cache import SentimentCache
import coordination_pb2

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("pika").propagate = False
logging.getLogger("google.protobuf").setLevel(logging.ERROR)
logging.getLogger("google.protobuf").propagate = False
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentiment")

RABBITMQ_URL = "amqp://admin:admin@rabbitmq:5672/"
QUEUE_NAME = "movies3"
SENTIMENT_EXCHANGE = "sentiment_exchange"
POSITIVE_QUEUE = "positive_movies"
NEGATIVE_QUEUE = "negative_movies"
COORDINATION_EXCHANGE = "coordination_exchange"
COORDINATION_KEY = "nlp"
NODE_ID = socket.gethostname()

ack_received = dict()
ack_events = dict()

leading_for = set()
not_leading_for = set()
sent_ack = set()


def get_event(client_id):
    if client_id not in ack_events:
        ack_events[client_id] = threading.Event()
    return ack_events[client_id]


def get_ack_received(client_id):
    if client_id not in ack_received:
        ack_received[client_id] = set()
    return ack_received[client_id]


def connect_rabbitmq_with_retries(retries=20, delay=3):
    for attempt in range(1, retries + 1):
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            return conn
        except pika.exceptions.AMQPConnectionError as e:
            logger.info(f"Attempt {attempt}: Could not connect to RabbitMQ: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    logger.error("Failed to connect to RabbitMQ after multiple attempts.")
    sys.exit(1)


def collect_ack(client_id, node_id):
    get_ack_received(client_id).add(node_id)
    logger.info(f"[client_id:{client_id}][leader] Received ACK from node {node_id}")
    if len(get_ack_received(client_id)) == expected_acks():
        get_event(client_id).set()


def cached_sentiment(movie):
    cached = cache.get(movie)
    if cached:
        return cached
    else:
        result = sentiment_analyzer(movie.overview)
        label = result[0]["label"]
        cache.set(movie, label)
        return label


def expected_acks():
    return int(getenv("NLP_NODES")) - 1


def wait_for_acks(client_id, timeout=10):
    def ack_waiter():
        logger.info(f"[client_id:{client_id}][leader] Waiting for ACKs")
        if get_event(client_id).wait(timeout):  # TODO only blocking the thread here?
            logger.info(f"[client_id:{client_id}][leader] All ACKs received")
        else:
            logger.warning(f"[client_id:{client_id}][leader] Timeout waiting for ACKs {get_ack_received(client_id)}")

    # Start the waiter in a new thread
    threading.Thread(target=ack_waiter).start()


def coordination_callback(ch, method, properties, body):
    message = coordination_pb2.CoordinationMessage()
    message.ParseFromString(body)
    if message.type == coordination_pb2.ACK and message.client_id in leading_for:
        collect_ack(message.client_id, message.node_id)
    elif message.type == coordination_pb2.LEADER:
        if message.node_id != NODE_ID:
            logger.info(f"[client_id:{message.client_id}][node] Received LEADER from {message.node_id}")
            not_leading_for.add(message.client_id)


def broadcast_leader_message(client_id):
    message = coordination_pb2.CoordinationMessage()
    message.client_id = client_id
    message.node_id = NODE_ID
    message.type = coordination_pb2.LEADER
    channel.basic_publish(
        exchange=COORDINATION_EXCHANGE,
        routing_key=COORDINATION_KEY,
        body=message.SerializeToString()
    )
    leading_for.add(client_id)
    logger.info(f"[client_id:{client_id}][leader] Broadcast LEADER message")


def send_ack(client_id):
    message = coordination_pb2.CoordinationMessage()
    message.client_id = client_id
    message.node_id = NODE_ID
    message.type = coordination_pb2.ACK
    channel.basic_publish(
        exchange=COORDINATION_EXCHANGE,
        routing_key=COORDINATION_KEY,
        body=message.SerializeToString()
    )
    logger.info(f"[client_id:{client_id}][node] Sent ACK message")


def callback(_, __, ___, body):
    movie = movie_sanit_pb2.MovieSanit()
    movie.ParseFromString(body)

    if movie.HasField("eof") and movie.eof:
        logger.info(f"[client_id:{movie.clientId}][leader] Received EOF")

        broadcast_leader_message(movie.clientId)
        wait_for_acks(movie.clientId)

        movie.eof = True
        eof_message = movie.SerializeToString()
        channel.basic_publish(exchange=SENTIMENT_EXCHANGE, routing_key=POSITIVE_QUEUE, body=eof_message)
        channel.basic_publish(exchange=SENTIMENT_EXCHANGE, routing_key=NEGATIVE_QUEUE, body=eof_message)
        return

    label = cached_sentiment(movie)
    target_queue = POSITIVE_QUEUE if label == "POSITIVE" else NEGATIVE_QUEUE

    channel.basic_publish(
        exchange=SENTIMENT_EXCHANGE,
        routing_key=target_queue,
        body=movie.SerializeToString()
    )

    # TODO // send ack for all that is not the current client
    if movie.clientId not in not_leading_for:
        for client_id in not_leading_for:
            send_ack(client_id)
        not_leading_for.clear()

    # logger.info(f"[client_id:{movie.clientId}] message published to {target_queue}")


def main():
    global sentiment_analyzer, channel

    logger.info("Connecting to RabbitMQ...")
    connection = connect_rabbitmq_with_retries()
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.exchange_declare(exchange=SENTIMENT_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(queue=POSITIVE_QUEUE, durable=True)
    channel.queue_declare(queue=NEGATIVE_QUEUE, durable=True)
    channel.queue_bind(exchange=SENTIMENT_EXCHANGE, queue=POSITIVE_QUEUE, routing_key=POSITIVE_QUEUE)
    channel.queue_bind(exchange=SENTIMENT_EXCHANGE, queue=NEGATIVE_QUEUE, routing_key=NEGATIVE_QUEUE)

    channel.exchange_declare(exchange=COORDINATION_EXCHANGE, exchange_type='topic', durable=True)
    queue = channel.queue_declare("", exclusive=True, auto_delete=True)
    channel.queue_bind(exchange=COORDINATION_EXCHANGE, queue=queue.method.queue, routing_key=COORDINATION_KEY)
    channel.basic_consume(queue=queue.method.queue, on_message_callback=coordination_callback, auto_ack=True)

    logger.info("Loading sentiment analysis model...")
    sentiment_analyzer = pipeline(
        task='sentiment-analysis',
        model='distilbert-base-uncased-finetuned-sst-2-english',
        max_length=64,
        truncation=True,
    )

    try:
        with SentimentCache(getenv("NODE_NUM")) as c:
            global cache
            cache = c
            logger.info(f"Listening for messages from queue '{QUEUE_NAME}'...")
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Interrupted. Closing connection...")
        channel.stop_consuming()
        connection.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
