from config.spark_config import SparkConnect, get_spark_config
from pyspark.sql.types import *
from spark_write_mysql import SparkWriteMySQL
#from src.spark.spark_write_mongo import SparkWriteMongoDB


db_config = get_spark_config()
mysql_config = db_config["mysql"]
mongodb_config = db_config["mongodb"]
jars = ["mysql:mysql-connector-java:8.0.33", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"]
spark_connect = SparkConnect(
        app_name="hoangduy",
        master_url="local[*]",
        executor_memory="2g",
        executor_cores=1,
        driver_memory="1g",
        num_executors=1,
        jar_packages=jars,
        spark_conf= {"spark.mongodb.write.connection.uri" : mongodb_config["uri"]},
        log_level="INFO"
    )
data = [
    ("Alice", 30, "New York"),
    ("Bob", 35, "London"),
    ("Charlie", 40, "Paris")
]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])


# 4. Create the DataFrame
df = spark_connect.spark.createDataFrame(data, schema)
df.show()
df_write_test_mysql = SparkWriteMySQL(spark_connect.spark, mysql_config) \
                .spark_write_mysql(df,mysql_config, mysql_config["jdbc"], "HOANGDUY", mode="overwrite")


#
# df_write_test_mongodb = SparkWriteMongoDB(spark_connect.spark, mongodb_config) \
#                 .spark_write_mongodb(df, mongodb_config["database"], mongodb_config["uri"], "DATDEPZAI", mode="overwrite")