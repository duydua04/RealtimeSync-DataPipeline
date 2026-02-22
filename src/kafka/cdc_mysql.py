import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType

class KafkaPureConnect:
    def __init__(self, spark: SparkSession, schema: StructType):
        self.kafka_host = os.getenv("KAFKA_HOST", "localhost")
        self.kafka_port = os.getenv("KAFKA_PORT", "9092")
        self.kafka_get_topic = os.getenv("KAFKA_DATA_TOPIC", "mysql_raw_topic")

        self.spark_session = spark
        self.raw_msg = self.get_data_change(schema)

    def get_data_change(self, schema: StructType) -> DataFrame:
        # 1. Kết nối đọc luồng từ Kafka KRaft
        query = self.spark_session.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", f"{self.kafka_host}:{self.kafka_port}") \
            .option("subscribe", self.kafka_get_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Ép kiểu Binary của Kafka về String
        raw_json = query.selectExpr("CAST(value AS STRING)")

        # 3. Dùng Schema THỰC TẾ của bảng để parse JSON (Không còn Schema Debezium khổng lồ nữa)
        parsed_df = raw_json.select(from_json(col("value"), schema).alias("data"))

        # 4. Định dạng lại cấu trúc để tương thích với class DataMigration phía sau
        # Vì dùng Kafka thuần, ta mặc định mọi tin nhắn nhận được đều là Thêm mới hoặc Cập nhật (Upsert)
        changes = parsed_df.select(
            col("data").alias("data_after"),
            lit(None).cast(schema).alias("data_before"), # Để Null vì ta không có data cũ
            lit("upsert").alias("data_change") # Gán cứng nhãn hành động là upsert
        ).filter(col("data_after").isNotNull())

        return changes

    @staticmethod
    def start_streaming(message: DataFrame, func, mode: str):
        message.writeStream \
            .foreachBatch(func) \
            .outputMode(mode) \
            .start().awaitTermination()