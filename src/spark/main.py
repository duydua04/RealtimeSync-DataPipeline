from config.spark_config import SparkConnect
from pyspark.sql.types import *

def main():
    SparkConnect.create_spark_session()