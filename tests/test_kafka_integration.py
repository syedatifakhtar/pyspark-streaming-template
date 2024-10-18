import pytest
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from retrying import retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=5, wait_fixed=5000)
def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

@retry(stop_max_attempt_number=5, wait_fixed=5000)
def create_consumer(bootstrap_servers):
    return KafkaConsumer(
        'foobar',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

def test_kafka_producer_consumer(kafka_zookeeper_containers):
    logger.info("Starting Kafka producer-consumer test")
    assert kafka_zookeeper_containers is not None, "Kafka container fixture was not initialized"
    try:
        bootstrap_servers = ['localhost:9092']
        logger.info(f"Using bootstrap servers: {bootstrap_servers}")
        
        # Create a producer
        producer = create_producer(bootstrap_servers)
        logger.info("Producer created successfully")
        
        # Send multiple messages
        test_messages = [
            {"id": 1, "message": "Hello"},
            {"id": 2, "message": "World"},
            {"id": 3, "message": "Kafka"}
        ]
        for msg in test_messages:
            future = producer.send("foobar", msg)
            result = future.get(timeout=60)  # wait for the message to be sent
            logger.info(f"Message sent successfully: {result}")
        producer.flush()
        
        # Create a consumer
        consumer = create_consumer(bootstrap_servers)
        logger.info("Consumer created successfully")
        
        # Consume messages with a timeout
        received_messages = []
        timeout = time.time() + 30  # 30 seconds from now
        while len(received_messages) < len(test_messages):
            for message in consumer:
                received_messages.append(message.value)
                logger.info(f"Message received: {message.value}")
                if len(received_messages) == len(test_messages):
                    break
            if time.time() > timeout:
                break
        
        # Assert that all messages were received
        assert len(received_messages) == len(test_messages), f"Expected {len(test_messages)} messages, but received {len(received_messages)}"
        assert all(msg in received_messages for msg in test_messages), "Not all sent messages were received"
        
    except KafkaError as e:
        logger.error(f"Kafka error occurred: {e}")
        raise
    finally:
        if 'producer' in locals():
            producer.close()
        if 'consumer' in locals():
            consumer.close()
