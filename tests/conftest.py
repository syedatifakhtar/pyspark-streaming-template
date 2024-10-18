import os
import tempfile
from functools import lru_cache
import docker
import time

import findspark
import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def kafka_zookeeper_containers():
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    
    # Remove existing containers if they exist
    for container_name in ["kafka", "zookeeper"]:
        try:
            container = client.containers.get(container_name)
            container.stop()
            container.remove()
            logger.info(f"Removed existing {container_name} container")
        except docker.errors.NotFound:
            pass

    # Remove existing network if it exists
    try:
        network = client.networks.get("kafka-net")
        network.remove()
        logger.info("Removed existing kafka-net network")
    except docker.errors.NotFound:
        pass

    # Create a new network
    network = client.networks.create("kafka-net", driver="bridge")
    logger.info("Created new kafka-net network")
    
    # Start Zookeeper
    zookeeper_container = client.containers.run(
        "wurstmeister/zookeeper:latest",
        detach=True,
        name="zookeeper",
        network="kafka-net",
        ports={'2181/tcp': 2181}
    )
    logger.info("Started Zookeeper container")
    
    # Wait for Zookeeper to start
    time.sleep(10)
    
    # Start Kafka
    kafka_container = client.containers.run(
        "wurstmeister/kafka:latest",
        detach=True,
        name="kafka",
        network="kafka-net",
        ports={'9092/tcp': 9092},
        environment={
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://localhost:9092",
            "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092",
            "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
            "KAFKA_CREATE_TOPICS": "foobar:1:1"
        }
    )
    
    # Wait for Kafka to start
    time.sleep(20)
    
    # Log container info
    logger.info(f"Zookeeper container ID: {zookeeper_container.id}")
    logger.info(f"Kafka container ID: {kafka_container.id}")
    logger.info(f"Kafka container logs: {kafka_container.logs().decode()}")
    
    yield kafka_container
    
    # Cleanup
    kafka_container.stop()
    kafka_container.remove()
    zookeeper_container.stop()
    zookeeper_container.remove()
    network.remove()


@pytest.fixture(scope="session")
def spark(request: FixtureRequest) -> SparkSession:
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[*] pyspark-shell"

    findspark.init()

    """
    Builds a spark session object
    For more information about configuration properties, see https://spark.apache.org/docs/latest/configuration.html
    :return: a SparkSession
    """
    spark_session: SparkSession = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.driver.memory", "3G")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config(
            "spark.sql.warehouse.dir",
            f"{tempfile.gettempdir()}/pyspark-template-test/spark-warehouse",
        )
        .appName("pyspark-template-test")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark_session.stop())

    return spark_session
