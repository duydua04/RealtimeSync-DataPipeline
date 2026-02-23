from pyspark.sql import SparkSession, DataFrame
from typing import Dict

class SparkWriteToMongodb:
    def __init__(self, spark: SparkSession, mongodb_config: Dict):
        self.spark = spark
        self.mongodb_config = mongodb_config

    def sparkwritetomongodb(self, df: DataFrame, uri: str, database: str, collection: str, mode: str):
            df.write.format("mongodb") \
            .option("collection", collection) \
            .option("connection.uri", uri) \
            .option("database", database) \
            .mode(mode) \
            .save()
            print(f'Successfully wrote to collection {collection}')