from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lower, struct
from pyspark.sql.types import *
from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_mongodb import SparkWriteToMongodb


def main():
    configs = get_spark_config()
    MONGO_URI = configs["mongodb"]["uri"]
    MONGO_DB = configs["mongodb"]["database"]
    KAFKA_SERVERS = "localhost:9092"

    jars = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ]

    spark_conn = SparkConnect(
        app_name="Realtime_CDC_To_MongoDB",
        master_url="local[*]",
        jar_packages=jars,
        log_level="WARN"
    )
    spark = spark_conn.spark
    mongo_writer = SparkWriteToMongodb(spark, configs["mongodb"])

    super_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("log_timestamp", StringType(), True),
        StructField("message_id", StringType(), True),
        StructField("hash", StringType(), True),
        # USERS
        StructField("user_id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("type", StringType(), True),
        StructField("site_admin", IntegerType(), True),
        # REPOS
        StructField("repo_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_private", IntegerType(), True),
        StructField("is_fork", IntegerType(), True),
        StructField("homepage", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("language", StringType(), True),
        StructField("forks_count", IntegerType(), True),
        StructField("stargazers_count", IntegerType(), True),
        StructField("watchers_count", IntegerType(), True),
        StructField("default_branch", StringType(), True),
        StructField("pushed_at", StringType(), True),
        # ORGS
        StructField("org_id", LongType(), True),
        # EVENTS
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("actor_id", LongType(), True),
        StructField("is_public", IntegerType(), True),
        StructField("forkee_repo_id", LongType(), True),
        # SHARED
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        # OWNERSHIP
        StructField("owner_user_id", LongType(), True),
        StructField("owner_org_id", LongType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", "spark_input") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .load()

    df_parsed = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), super_schema).alias("data")) \
        .select("data.*")

    def process_every_batch(df, batch_id):
        valid_df = df.filter(col("table_name").isNotNull())
        valid_df = valid_df.filter(col("table_name") != "repo_ownership")

        if valid_df.isEmpty():
            return

        distinct_tables = [row['table_name'].lower() for row in valid_df.select("table_name").distinct().collect()]

        for table in distinct_tables:
            table_df = valid_df.filter(lower(col("table_name")) == table)

            clean_columns = [c for c in table_df.columns if table_df.where(col(c).isNotNull()).count() > 0]
            display_df = table_df.select(*clean_columns)

            print(f"\n[BATCH: {batch_id}] - DATA DETECTED IN TABLE: {table.upper()}")
            display_df.show(truncate=False)

            write_df = table_df.drop("table_name", "hash", "message_id", "log_timestamp")

            if table == "users":
                write_df = table_df.select(
                    col("user_id").alias("_id"),
                    "status",
                    "login",
                    "avatar_url",
                    "url",
                    "type",
                    col("site_admin").cast("boolean"),
                    "created_at"
                )
            elif table == "repos":
                write_df = table_df.select(
                    col("repo_id").alias("_id"),
                    "status",
                    "name",
                    "full_name",
                    "description",
                    col("is_private").cast("boolean"),
                    col("is_fork").cast("boolean"),
                    "language",
                    "size",
                    "created_at"
                )
            elif table == "orgs":
                write_df = table_df.select(
                    col("org_id").alias("_id"),
                    "status",
                    "login",
                    "avatar_url"
                )
            elif table == "events":
                write_df = table_df.select(
                    col("event_id").alias("_id"),
                    "status",
                    col("event_type").alias("type"),
                    "actor_id",
                    "repo_id",
                    "org_id",
                    col("is_public").cast("boolean"),
                    "created_at"
                )

            try:
                mongo_writer.sparkwritetomongodb(
                    df=write_df,
                    uri=MONGO_URI,
                    database=MONGO_DB,
                    collection=table,
                    mode="append"
                )
                print(f"Synced {table} to MongoDB.")
            except Exception as e:
                print(f"Error writing {table}: {e}")

    query = df_parsed.writeStream \
        .foreachBatch(process_every_batch) \
        .option("checkpointLocation", "/home/hoangduy/PycharmProjects/DataPipeline/checkpoint/mongodb_sink") \
        .start()

    print("Stream is active and waiting for data from Kafka...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()