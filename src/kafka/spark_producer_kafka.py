from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
# Import class SparkConnect của bạn vào đây
from config.spark_config import SparkConnect, get_spark_config


def main():
    required_jars = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ]

    spark_conn = SparkConnect(
        app_name="Processing",
        master_url="local[*]",
        executor_memory="4g",
        jar_packages=required_jars,
        log_level="WARN"
    )

    spark = spark_conn.spark

    spark_db_config = get_spark_config()

    # Định nghĩa Schema
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status", StringType(), True),
        StructField("log_timestamp", StringType(), True),
        StructField("message_id", StringType(), True),
        StructField("hash", StringType(), True)
    ])

    print("🚀 Bắt đầu đọc dữ liệu Streaming từ Kafka...")

    # Đọc từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "spark_input") \
        .option("startingOffsets", "earliest") \
        .load()

    # Bóc tách JSON
    df_property = kafka_df.select(col("value").cast("string")) \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # In ra Console
    console_query = df_property.writeStream \
        .format("console") \
        .queryName("ConsoleOutput") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("append") \
        .start()

    console_query.awaitTermination()


if __name__ == "__main__":
    main()