
import pyspark.sql.functions as f
from pyspark.sql.functions import explode, split, col, count, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pyspark.sql import DataFrame, SparkSession

from pyspark_template.utils.spark import get_spark_session


def main() -> None:
    # Define the schema for the incoming data
    spark = get_spark_session()
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("value", StringType(), True)
    ])

    # Create DataFrame representing the stream of input lines from socket
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # Split the lines into words
    words = lines.select(
        explode(split(col("value"), " ")).alias("word")
    )

    # Generate running word count
    word_counts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = word_counts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()
