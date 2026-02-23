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

        self.validate_data(df, mysql_table, jdbc, config)

    def validate_data(self, df_source: DataFrame, mysql_table: str, jdbc: str, config: Dict):
        """
        Thực hiện đối soát số lượng dòng giữa DataFrame nguồn và bảng đích trong MySQL
        """
        print(f"Starting validation for table: {mysql_table}...")

        df_target = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc) \
            .option("dbtable", mysql_table) \
            .option("user", config["config"]["user"]) \
            .option("password", config["config"]["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        source_count = df_source.count()
        target_count = df_target.count()

        if source_count == target_count:
            print(f"VALIDATION SUCCESS: {source_count} records matched perfectly.")
        else:
            diff = abs(source_count - target_count)
            print(f"VALIDATION FAILED: Source has {source_count} but Target has {target_count}.")
            print(f"Gap: {diff} records missing or duplicated.")

            missing_df = df_source.exceptAll(df_target)
            if not missing_df.isEmpty():
                print(f"Displaying some missing records:")
                missing_df.show(5)
