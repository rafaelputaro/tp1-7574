import logging
import os
import time
import pika
import sys
from transformers import pipeline
import movie_sanit_pb2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentiment")

RABBITMQ_URL = "amqp://admin:admin@rabbitmq:5672/"
FANOUT_EXCHANGE = "movies_exchange"
POSITIVE_QUEUE_PREFIX = "positive_movie_reviews_"
NEGATIVE_QUEUE_PREFIX = "negative_movie_reviews_"
NUM_SHARDS = int(os.getenv("NUM_SHARDS", 1))
NODE_NUM = int(os.getenv("NODE_NUM"))

def calculate_shard(movie_id: int, num_shards: int) -> int:
    """Calculate the shard index based on the movie ID."""
    return (movie_id % num_shards) + 1

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

def callback(ch, method, properties, body):
    movie = movie_sanit_pb2.MovieSanit()
    movie.ParseFromString(body)

    if movie.HasField("eof") and movie.eof:
        logger.info("Received EOF. Sending EOF message to all output queues...")

        eof_message = movie.SerializeToString()
        for shard in range(1, NUM_SHARDS + 1):
            pos_queue = f"{POSITIVE_QUEUE_PREFIX}{shard}"
            neg_queue = f"{NEGATIVE_QUEUE_PREFIX}{shard}"

            channel.basic_publish(exchange='', routing_key=pos_queue, body=eof_message)
            channel.basic_publish(exchange='', routing_key=neg_queue, body=eof_message)

        logger.info("EOF messages sent to all output queues. Stopping consumption.")
        ch.stop_consuming()
        return

    text = movie.overview

    result = sentiment_analyzer(text)
    label = result[0]["label"]

    shard = calculate_shard(movie.id, NUM_SHARDS)
    queue_prefix = POSITIVE_QUEUE_PREFIX if label == "POSITIVE" else NEGATIVE_QUEUE_PREFIX
    target_queue = f"{queue_prefix}{shard}"

    channel.basic_publish(
        exchange='',
        routing_key=target_queue,
        body=movie.SerializeToString()
    )

    logger.info(f"Message published to {target_queue} (shard {shard})")

def main():
    global sentiment_analyzer, channel

    logger.info("Connecting to RabbitMQ...")
    connection = connect_rabbitmq_with_retries(logger)
    channel = connection.channel()

    # Declare the fanout exchange
    channel.exchange_declare(exchange=FANOUT_EXCHANGE, exchange_type='fanout', durable=True)

    # Declare a unique, temporary queue
    queue_name = f"sentiment_temp_queue_{NODE_NUM}"
    result = channel.queue_declare(queue='', exclusive=True, durable=True)
    queue_name = result.method.queue

    # Bind it to the fanout exchange
    channel.queue_bind(exchange=FANOUT_EXCHANGE, queue=queue_name)

    # Ensure target queues exist
    for shard in range(1, NUM_SHARDS + 1):
        channel.queue_declare(queue=f"{POSITIVE_QUEUE_PREFIX}{shard}", durable=True)
        channel.queue_declare(queue=f"{NEGATIVE_QUEUE_PREFIX}{shard}", durable=True)

    logger.info("Loading sentiment analysis model...")
    sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

    logger.info(f"Listening for messages from exchange '{FANOUT_EXCHANGE}'...")
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Interrupted. Closing connection...")
        channel.stop_consuming()
        connection.close()
        sys.exit(0)

if __name__ == "__main__":
    main()