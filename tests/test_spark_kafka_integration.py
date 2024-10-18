import pytest
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from kafka import KafkaProducer
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_spark_kafka_streaming(kafka_zookeeper_containers, spark):
    logger.info("Starting Spark Kafka Streaming test")
    assert kafka_zookeeper_containers is not None, "Kafka container fixture was not initialized"
    
    try:
        # Define schema for incoming messages
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("message", StringType(), True)
        ])

        # Create a producer and send messages
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_messages = [
            {"id": 1, "message": "Hello"},
            {"id": 2, "message": "World"},
            {"id": 3, "message": "Kafka"}
        ]
        
        for msg in test_messages:
            future = producer.send("input-topic", msg)
            result = future.get(timeout=60)
            logger.info(f"Message sent: {msg}, offset: {result.offset}")
        producer.flush()
        logger.info("All test messages sent to Kafka")

        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "input-topic") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse the JSON data
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        # Process the data (in this case, we'll just add a "processed" field)
        processed_df = parsed_df.withColumn("processed", col("message").cast("string"))

        # Collect the processed data in memory
        query = processed_df \
            .writeStream \
            .outputMode("append") \
            .format("memory") \
            .queryName("processed_messages") \
            .start()

        # Wait for the streaming query to process the data
        timeout = time.time() + 60  # 60 seconds from now
        while time.time() < timeout:
            count = spark.sql("SELECT COUNT(*) FROM processed_messages").collect()[0][0]
            logger.info(f"Current count of processed messages: {count}")
            if count >= len(test_messages):
                break
            time.sleep(5)

        # Stop the streaming query
        query.stop()

        # Retrieve the processed messages
        received_messages = spark.sql("SELECT * FROM processed_messages").collect()
        logger.info(f"Received messages: {received_messages}")

        # Assert that all messages were processed and received
        assert len(received_messages) == len(test_messages), f"Expected {len(test_messages)} messages, but received {len(received_messages)}"
        for sent, received in zip(test_messages, received_messages):
            assert sent["id"] == received["id"], f"Message ID mismatch: sent {sent['id']}, received {received['id']}"
            assert sent["message"] == received["message"], f"Message content mismatch: sent {sent['message']}, received {received['message']}"
            assert received["processed"] is not None, f"Processed field missing in received message: {received}"

        logger.info("Spark Kafka Streaming test completed successfully")

    except Exception as e:
        logger.error(f"Error in Spark Kafka Streaming test: {e}")
        raise
    finally:
        if 'producer' in locals():
            producer.close()
        if 'query' in locals() and query.isActive:
            query.stop()
