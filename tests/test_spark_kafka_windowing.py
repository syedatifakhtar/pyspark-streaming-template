import pytest
import time
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, window, sum as spark_sum, collect_list, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from kafka import KafkaProducer
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ignore_test_spark_kafka_windowing(kafka_zookeeper_containers, spark):
    logger.info("Starting Spark Kafka Windowing test")
    assert kafka_zookeeper_containers is not None, "Kafka container fixture was not initialized"
    
    try:
        # Define schema for incoming messages
        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("value", IntegerType(), True)
        ])

        # Create a producer and send messages
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        base_time = datetime.now()
        test_messages = [
            {"timestamp": base_time, "value": 1},
            {"timestamp": base_time + timedelta(seconds=30), "value": 2},
            {"timestamp": base_time + timedelta(seconds=50), "value": 3},
            {"timestamp": base_time + timedelta(minutes=2), "value": 4},
            {"timestamp": base_time + timedelta(minutes=3), "value": 5}
        ]

         # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "windowing-topic") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse the JSON data
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        # Apply windowing and aggregation
        windowed_df = parsed_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(window("timestamp", "5 minutes", "1 minute")) \
            .agg(spark_sum("value").alias("total_value"))

        # Collect the processed data in memory
        query = windowed_df \
            .writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("windowed_messages") \
            .start()
        
        
        for msg in test_messages:
            future = producer.send("windowing-topic", msg)
            result = future.get(timeout=60)
            logger.info(f"Message sent: {msg}, offset: {result.offset}")
        producer.flush()
        logger.info("All test messages sent to Kafka")


        # Wait for the streaming query to process the data
        timeout = time.time() + 60  # 60 seconds from now
        while time.time() < timeout:
            count = spark.sql("SELECT COUNT(*) FROM windowed_messages").collect()[0][0]
            logger.info(f"Current count of windowed results: {count}")
            if count > 0:
                break
            time.sleep(5)

        # Stop the streaming query
        query.stop()

        # Retrieve the processed messages and sort them by window start time
        windowed_results = spark.sql("SELECT * FROM windowed_messages").collect()
        sorted_results = sorted(windowed_results, key=lambda x: x['window']['start'])
        
        logger.info("Sorted Windowed results:")
        for row in sorted_results:
            window_start = row['window']['start']
            window_end = row['window']['end']
            total_value = row['total_value']
            logger.info(f"Window: {window_start} to {window_end}, Total Value: {total_value}")

        # Assert the windowed results
        assert len(sorted_results) > 0, "No windowed results were produced"
        
        # Check each window
        for row in sorted_results:
            window_start = row['window']['start']
            window_end = row['window']['end']
            total_value = row['total_value']
            
            # Calculate expected sum for this window
            expected_sum = sum(msg['value'] for msg in test_messages 
                               if window_start <= msg['timestamp'] < window_end)
            
            assert total_value == expected_sum, f"Window from {window_start} to {window_end}: Expected sum of {expected_sum}, got {total_value}"

        logger.info("Spark Kafka Windowing test completed successfully")

    except Exception as e:
        logger.error(f"Error in Spark Kafka Windowing test: {e}")
        raise
    finally:
        if 'producer' in locals():
            producer.close()
        if 'query' in locals() and query.isActive:
            query.stop()


def test_spark_custom_session_window(kafka_zookeeper_containers, spark):
    logger.info("Starting Spark Custom Session Window test")
    assert kafka_zookeeper_containers is not None, "Kafka container fixture was not initialized"
    
    try:
        # Define schema for incoming messages
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", IntegerType(), True)
        ])

        # Create a producer and send messages
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        base_time = datetime.now()
        test_messages = [
            {"id": "A", "timestamp": base_time, "value": 1},
            {"id": "B", "timestamp": base_time + timedelta(minutes=5), "value": 2},
            {"id": "A", "timestamp": base_time + timedelta(hours=2), "value": 3},
            {"id": "B", "timestamp": base_time + timedelta(hours=23), "value": 4},
            {"id": "A", "timestamp": base_time + timedelta(hours=25), "value": 5}  # This should start a new session for A
        ]
        
        for msg in test_messages:
            future = producer.send("custom-session-topic", msg)
            result = future.get(timeout=60)
            logger.info(f"Message sent: {msg}, offset: {result.offset}")
        producer.flush()
        logger.info("All test messages sent to Kafka")

        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "custom-session-topic") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse the JSON data
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        # Define a UDF for custom session aggregation
        @udf(returnType=ArrayType(StructType([
            StructField("timestamp", TimestampType()),
            StructField("value", IntegerType())
        ])))
        def session_aggregate(timestamps, values):
            events = sorted(zip(timestamps, values), key=lambda x: x[0])
            sessions = []
            current_session = []
            for timestamp, value in events:
                if not current_session or (timestamp - current_session[-1][0]).total_seconds() <= 86400:  # 24 hours in seconds
                    current_session.append((timestamp, value))
                else:
                    sessions.append(current_session)
                    current_session = [(timestamp, value)]
            if current_session:
                sessions.append(current_session)
            return [{"timestamp": s[0][0], "value": sum(v for _, v in s)} for s in sessions]

        # Apply custom windowing
        windowed_df = parsed_df \
            .groupBy("id") \
            .agg(
                collect_list("timestamp").alias("timestamps"),
                collect_list("value").alias("values")
            ) \
            .select(
                "id",
                session_aggregate("timestamps", "values").alias("sessions")
            )

        # Explode the sessions to get individual rows
        exploded_df = windowed_df.select("id", "sessions.timestamp", "sessions.value")

        # Collect the processed data in memory
        query = exploded_df \
            .writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("custom_session_messages") \
            .start()

        # Wait for the streaming query to process the data
        timeout = time.time() + 60  # 60 seconds from now
        while time.time() < timeout:
            count = spark.sql("SELECT COUNT(*) FROM custom_session_messages").collect()[0][0]
            logger.info(f"Current count of custom session results: {count}")
            if count > 0:
                break
            time.sleep(5)

        # Stop the streaming query

        query.stop()

        # Retrieve and log the processed messages
        session_results = spark.sql("SELECT * FROM custom_session_messages").collect()
        logger.info("Custom Session results:")
        logger.info("=" * 50)
        for row in session_results:
            logger.info(f"ID: {row['id']} | Timestamp: {row['timestamp']} | Aggregated Value: {row['value']}")
        logger.info("=" * 50)

        # Assert the custom session results
        assert len(session_results) > 0, "No custom session results were produced"
        
        # Check specific session results
        id_a_sessions = [row for row in session_results if row['id'] == 'A']
        id_b_sessions = [row for row in session_results if row['id'] == 'B']
        
        logger.info("Assertion Results:")
        logger.info("-" * 50)
        
        # Assert and log session counts
        a_session_count = len(id_a_sessions)
        b_session_count = len(id_b_sessions)
        logger.info(f"ID A Sessions: Expected 2, Got {a_session_count}")
        logger.info(f"ID B Sessions: Expected 1, Got {b_session_count}")
        assert a_session_count == 2, f"Expected 2 sessions for ID A, got {a_session_count}"
        assert b_session_count == 1, f"Expected 1 session for ID B, got {b_session_count}"
        
        # Assert and log session values
        logger.info("Session Values:")
        for i, session in enumerate(id_a_sessions):
            expected_value = 4 if i == 0 else 5
            actual_value = session['value']
            logger.info(f"  ID A Session {i+1}: Expected {expected_value}, Got {actual_value}")
            assert actual_value == expected_value, f"Expected sum of {expected_value} for session {i+1} of ID A, got {actual_value}"
        
        b_session_value = id_b_sessions[0]['value']
        logger.info(f"  ID B Session: Expected 6, Got {b_session_value}")
        assert b_session_value == 6, f"Expected sum of 6 for session of ID B, got {b_session_value}"
        
        logger.info("-" * 50)
        logger.info("All assertions passed successfully")
        logger.info("Spark Custom Session Window test completed successfully")

    except Exception as e:
        logger.error(f"Error in Spark Custom Session Window test: {e}")
        raise
    finally:
        if 'producer' in locals():
            producer.close()
        if 'query' in locals() and query.isActive:
            query.stop()
