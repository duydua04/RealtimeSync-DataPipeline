from typing import Dict
from pyspark.sql import SparkSession, DataFrame

class SparkWriteMySQL:
    def __init__(self, spark: SparkSession, mysql_config: Dict):
        self.spark = spark
        self.mysql_config = mysql_config

    def spark_write_mysql(
            self, df: DataFrame,
            config: Dict, jdbc: str,
            mysql_table: str, mode: str
    ):
        df.write \
            .format("jdbc") \
            .option("url", jdbc) \
            .option("dbtable", mysql_table) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("user", config["config"]["user"]) \
            .option("password", config["config"]["password"]) \
            .mode(mode) \
            .save()

        print(f"Successfully wrote data to MySQL table: {mysql_table}")


