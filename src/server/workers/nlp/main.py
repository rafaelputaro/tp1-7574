import logging
import sys
import time
from os import getenv

import pika
from transformers import pipeline

import movie_sanit_pb2
from cache import SentimentCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentiment")

RABBITMQ_URL = "amqp://admin:admin@rabbitmq:5672/"
QUEUE_NAME = "movies3"
POSITIVE_QUEUE = "positive_movies"
NEGATIVE_QUEUE = "negative_movies"


def connect_rabbitmq_with_retries(logger: logging.Logger, retries=20, delay=3):
    """Attempt to connect to RabbitMQ with retries."""
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


sentiment_cache = {}


def cached_sentiment(movie):
    cached = cache.get(movie)
    if cached:
        return cached
    else:
        result = sentiment_analyzer(movie.overview)
        label = result[0]["label"]
        cache.set(movie, label)
        return label


def callback(ch, method, properties, body):
    movie = movie_sanit_pb2.MovieSanit()
    movie.ParseFromString(body)

    if movie.HasField("eof") and movie.eof:
        logger.info(f"[client_id:{movie.clientId}] received EOF")

        movie.eof = True
        eof_message = movie.SerializeToString()
        channel.basic_publish(exchange='sentiment_exchange', routing_key=POSITIVE_QUEUE, body=eof_message)
        channel.basic_publish(exchange='sentiment_exchange', routing_key=NEGATIVE_QUEUE, body=eof_message)

        # ch.stop_consuming()
        return

    label = cached_sentiment(movie)
    target_queue = POSITIVE_QUEUE if label == "POSITIVE" else NEGATIVE_QUEUE

    channel.basic_publish(
        exchange='sentiment_exchange',
        routing_key=target_queue,
        body=movie.SerializeToString()
    )

    logger.info(f"[client_id:{movie.clientId}] message published to {target_queue}")


def main():
    global sentiment_analyzer, channel

    logger.info("Connecting to RabbitMQ...")
    connection = connect_rabbitmq_with_retries(logger)
    channel = connection.channel()

    channel.basic_qos(prefetch_count=1)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.exchange_declare(exchange='sentiment_exchange', exchange_type='direct', durable=True)
    channel.queue_declare(queue=POSITIVE_QUEUE, durable=True)
    channel.queue_declare(queue=NEGATIVE_QUEUE, durable=True)
    channel.queue_bind(exchange='sentiment_exchange', queue=POSITIVE_QUEUE, routing_key=POSITIVE_QUEUE)
    channel.queue_bind(exchange='sentiment_exchange', queue=NEGATIVE_QUEUE, routing_key=NEGATIVE_QUEUE)

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
