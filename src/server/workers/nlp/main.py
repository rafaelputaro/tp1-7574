import logging
import sys
import time

import pika
from transformers import pipeline

import movie_sanit_pb2

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


# Add this global counter and max limit at the top of the file
MESSAGE_LIMIT = 2000
message_count = 0


def callback(ch, method, properties, body):
    global message_count
    movie = movie_sanit_pb2.MovieSanit()
    movie.ParseFromString(body)

    if (movie.HasField("eof") and movie.eof) or message_count >= MESSAGE_LIMIT:
        logger.info("Received EOF. Sending EOF message to all output queues...")

        movie.eof = True
        eof_message = movie.SerializeToString()
        channel.basic_publish(exchange='sentiment_exchange', routing_key=POSITIVE_QUEUE, body=eof_message)
        channel.basic_publish(exchange='sentiment_exchange', routing_key=NEGATIVE_QUEUE, body=eof_message)

        logger.info("EOF messages sent to all output queues. Stopping consumption.")
        ch.stop_consuming()
        return

    text = movie.overview

    result = sentiment_analyzer(text)
    label = result[0]["label"]
    target_queue = POSITIVE_QUEUE if label == "POSITIVE" else NEGATIVE_QUEUE

    channel.basic_publish(
        exchange='sentiment_exchange',
        routing_key=target_queue,
        body=movie.SerializeToString()
    )

    logger.info(f"Message published to {target_queue}")
    message_count += 1


def main():
    global sentiment_analyzer, channel

    logger.info("Connecting to RabbitMQ...")
    connection = connect_rabbitmq_with_retries(logger)
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.exchange_declare(exchange='sentiment_exchange', exchange_type='direct', durable=True)
    channel.queue_declare(queue=POSITIVE_QUEUE, durable=True)
    channel.queue_declare(queue=NEGATIVE_QUEUE, durable=True)
    channel.queue_bind(exchange='sentiment_exchange', queue=POSITIVE_QUEUE, routing_key=POSITIVE_QUEUE)
    channel.queue_bind(exchange='sentiment_exchange', queue=NEGATIVE_QUEUE, routing_key=NEGATIVE_QUEUE)

    logger.info("Loading sentiment analysis model...")
    sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english', max_length=512, truncation=True)

    logger.info(f"Listening for messages from queue '{QUEUE_NAME}'...")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Interrupted. Closing connection...")
        channel.stop_consuming()
        connection.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
