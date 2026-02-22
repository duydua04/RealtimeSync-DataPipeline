from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, lit, when

# Import từ thư viện cấu hình và kết nối của bạn
from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_mysql import SparkWriteMySQL  # Đổi lại đường dẫn import nếu bạn để file ở thư mục khác
from src.spark.spark_write_mongodb import SparkWriteToMongodb


# ==============================================================================
# 1. HÀM HELPER ĐỂ TRANSFORM DỮ LIỆU AN TOÀN
# ==============================================================================
def has_nested_field(df, path: str) -> bool:
    parts = path.split(".")
    curr_schema = df.schema
    for p in parts:
        if isinstance(curr_schema, StructType):
            f = next((x for x in curr_schema.fields if x.name == p), None)
            if f is None: return False
            curr_schema = f.dataType
        else:
            return False
    return True


def safe_col(df, path: str, cast_type: DataType, alias_name: str):
    if has_nested_field(df, path):
        return col(path).cast(cast_type).alias(alias_name)
    return lit(None).cast(cast_type).alias(alias_name)


# ==============================================================================
# 2. KHỞI TẠO SPARK VÀ CẤU HÌNH BẰNG MODULE `SparkConnect`
# ==============================================================================
def setup_spark_environment():
    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ]

    # Lấy config MongoDB để gán vào spark_conf
    db_config = get_spark_config()
    mongo_uri = db_config["mongodb"]["uri"]

    spark_connect = SparkConnect(
        app_name='Batch_Sync_MySQL_MongoDB',
        master_url='local[*]',
        executor_cores=3,
        executor_memory='2g',
        driver_memory='1g',
        num_executors=1,
        jar_packages=jars,
        spark_conf={"spark.mongodb.write.connection.uri": mongo_uri},
        log_level='ERROR'
    )
    return spark_connect.spark


# ==============================================================================
# 3. LUỒNG CHÍNH (ETL PROCESS)
# ==============================================================================
def run_batch_sync():
    # Khởi tạo Spark và lấy toàn bộ config từ thư mục config/
    spark = setup_spark_environment()
    app_configs = get_spark_config()

    # Biến đường dẫn file JSON
    JSON_FILE_PATH = "../data/small_json.json"

    print(f"Đang đọc dữ liệu từ: {JSON_FILE_PATH}")
    df_raw = spark.read.option("multiLine", "true").json(JSON_FILE_PATH)
    df = df_raw.withColumn("created_at_ts", to_timestamp(col("created_at")))

    print("Đang thực hiện Transform (Chuẩn hóa JSON thành 5 bảng)...")

    # 1. BẢNG USERS
    df_actor = df.select(
        safe_col(df, "actor.id", LongType(), "user_id"),
        safe_col(df, "actor.login", StringType(), "login"),
        safe_col(df, "actor.avatar_url", StringType(), "avatar_url"),
        safe_col(df, "actor.url", StringType(), "url"),
        lit("User").alias("type"),
        lit(False).alias("site_admin")
    ).filter(col("user_id").isNotNull())

    df_fork_owner = spark.createDataFrame([], df_actor.schema)
    if has_nested_field(df, "payload.forkee.owner"):
        df_fork_owner = df.select(
            safe_col(df, "payload.forkee.owner.id", LongType(), "user_id"),
            safe_col(df, "payload.forkee.owner.login", StringType(), "login"),
            safe_col(df, "payload.forkee.owner.avatar_url", StringType(), "avatar_url"),
            safe_col(df, "payload.forkee.owner.url", StringType(), "url"),
            safe_col(df, "payload.forkee.owner.type", StringType(), "type"),
            safe_col(df, "payload.forkee.owner.site_admin", BooleanType(), "site_admin")
        ).filter(col("user_id").isNotNull())
    df_users = df_actor.unionByName(df_fork_owner, allowMissingColumns=True).dropDuplicates(["user_id"])

    # 2. BẢNG ORGS
    df_orgs = spark.createDataFrame([], StructType([
        StructField("org_id", LongType()), StructField("login", StringType()),
        StructField("url", StringType()), StructField("avatar_url", StringType())
    ]))
    if has_nested_field(df, "org"):
        df_orgs = df.select(
            safe_col(df, "org.id", LongType(), "org_id"),
            safe_col(df, "org.login", StringType(), "login"),
            safe_col(df, "org.url", StringType(), "url"),
            safe_col(df, "org.avatar_url", StringType(), "avatar_url")
        ).filter(col("org_id").isNotNull()).dropDuplicates(["org_id"])

    # 3. BẢNG REPOS
    df_repo_base = df.select(
        safe_col(df, "repo.id", LongType(), "repo_id"),
        safe_col(df, "repo.name", StringType(), "name"),
        lit(None).cast(StringType()).alias("full_name"),
        safe_col(df, "repo.url", StringType(), "url"),
        lit(None).cast(StringType()).alias("html_url"),
        lit(None).cast(StringType()).alias("description"),
        lit(None).cast(BooleanType()).alias("is_private"),
        lit(None).cast(BooleanType()).alias("is_fork"),
        lit(None).cast(StringType()).alias("homepage"),
        lit(None).cast(IntegerType()).alias("size"),
        lit(None).cast(StringType()).alias("language"),
        lit(None).cast(IntegerType()).alias("forks_count"),
        lit(None).cast(IntegerType()).alias("stargazers_count"),
        lit(None).cast(IntegerType()).alias("watchers_count"),
        lit(None).cast(StringType()).alias("default_branch"),
        lit(None).cast(TimestampType()).alias("created_at"),
        lit(None).cast(TimestampType()).alias("updated_at"),
        lit(None).cast(TimestampType()).alias("pushed_at")
    ).filter(col("repo_id").isNotNull())

    df_repo_forkee = spark.createDataFrame([], df_repo_base.schema)
    if has_nested_field(df, "payload.forkee"):
        df_repo_forkee = df.select(
            safe_col(df, "payload.forkee.id", LongType(), "repo_id"),
            safe_col(df, "payload.forkee.name", StringType(), "name"),
            safe_col(df, "payload.forkee.full_name", StringType(), "full_name"),
            safe_col(df, "payload.forkee.url", StringType(), "url"),
            safe_col(df, "payload.forkee.html_url", StringType(), "html_url"),
            safe_col(df, "payload.forkee.description", StringType(), "description"),
            safe_col(df, "payload.forkee.private", BooleanType(), "is_private"),
            safe_col(df, "payload.forkee.fork", BooleanType(), "is_fork"),
            safe_col(df, "payload.forkee.homepage", StringType(), "homepage"),
            safe_col(df, "payload.forkee.size", IntegerType(), "size"),
            safe_col(df, "payload.forkee.language", StringType(), "language"),
            safe_col(df, "payload.forkee.forks_count", IntegerType(), "forks_count"),
            safe_col(df, "payload.forkee.stargazers_count", IntegerType(), "stargazers_count"),
            safe_col(df, "payload.forkee.watchers_count", IntegerType(), "watchers_count"),
            safe_col(df, "payload.forkee.default_branch", StringType(), "default_branch"),
            to_timestamp(col("payload.forkee.created_at")).alias("created_at"),
            to_timestamp(col("payload.forkee.updated_at")).alias("updated_at"),
            to_timestamp(col("payload.forkee.pushed_at")).alias("pushed_at")
        ).filter(col("repo_id").isNotNull())
    df_repos = df_repo_forkee.unionByName(df_repo_base).dropDuplicates(["repo_id"])

    # 4. BẢNG EVENTS
    df_events = df.select(
        safe_col(df, "id", StringType(), "event_id"),
        safe_col(df, "type", StringType(), "event_type"),
        safe_col(df, "actor.id", LongType(), "actor_id"),
        safe_col(df, "repo.id", LongType(), "repo_id"),
        safe_col(df, "org.id", LongType(), "org_id"),
        safe_col(df, "public", BooleanType(), "is_public"),
        col("created_at_ts").alias("created_at"),
        when(col("type") == "ForkEvent", safe_col(df, "payload.forkee.id", LongType(), None))
        .otherwise(lit(None)).alias("forkee_repo_id")
    ).dropDuplicates(["event_id"])

    # 5. BẢNG REPO_OWNERSHIP
    df_ownership_org = df.select(
        safe_col(df, "repo.id", LongType(), "repo_id"),
        lit(None).cast(LongType()).alias("owner_user_id"),
        safe_col(df, "org.id", LongType(), "owner_org_id")
    ).filter(col("repo_id").isNotNull() & col("owner_org_id").isNotNull())

    df_ownership_user = spark.createDataFrame([], df_ownership_org.schema)
    if has_nested_field(df, "payload.forkee"):
        df_ownership_user = df.select(
            safe_col(df, "payload.forkee.id", LongType(), "repo_id"),
            safe_col(df, "payload.forkee.owner.id", LongType(), "owner_user_id"),
            lit(None).cast(LongType()).alias("owner_org_id")
        ).filter(col("repo_id").isNotNull() & col("owner_user_id").isNotNull())
    df_repo_ownership = df_ownership_org.unionByName(df_ownership_user).dropDuplicates(["repo_id"])

    # --- LOAD: GHI VÀO CƠ SỞ DỮ LIỆU BẰNG CLASS IMPORT ---

    # Chuẩn bị Config cho Class MySQL
    mysql_writer = SparkWriteMySQL(spark, app_configs["mysql"])
    jdbc_url = app_configs["mysql"]["jdbc"]
    mysql_properties = app_configs["mysql"]  # Class của bạn yêu cầu tham số config nguyên cục

    # Chuẩn bị Config cho Class MongoDB
    mongo_writer = SparkWriteToMongodb(spark, app_configs["mongodb"])
    mongo_uri = app_configs["mongodb"]["uri"]
    mongo_db_name = app_configs["mongodb"]["database"]

    print("\n--- BẮT ĐẦU GHI VÀO MYSQL (Tuân thủ thứ tự Khóa ngoại) ---")
    mysql_writer.spark_write_mysql(df_users, mysql_properties, jdbc_url, "USERS", 'append')
    mysql_writer.spark_write_mysql(df_orgs, mysql_properties, jdbc_url, "ORGS", 'append')
    mysql_writer.spark_write_mysql(df_repos, mysql_properties, jdbc_url, "REPOS", 'append')
    mysql_writer.spark_write_mysql(df_events, mysql_properties, jdbc_url, "EVENTS", 'append')
    mysql_writer.spark_write_mysql(df_repo_ownership, mysql_properties, jdbc_url, "REPO_OWNERSHIP", 'append')

    print("\n--- BẮT ĐẦU GHI VÀO MONGODB ---")
    mongo_writer.sparkwritetomongodb(df_users, mongo_uri, mongo_db_name, "Users", 'append')
    mongo_writer.sparkwritetomongodb(df_orgs, mongo_uri, mongo_db_name, "Orgs", 'append')
    mongo_writer.sparkwritetomongodb(df_repos, mongo_uri, mongo_db_name, "Repos", 'append')
    mongo_writer.sparkwritetomongodb(df_events, mongo_uri, mongo_db_name, "Events", 'append')

    # Ghi toàn bộ Raw JSON vào Data Lake bên Mongo
    mongo_writer.sparkwritetomongodb(df_raw, mongo_uri, mongo_db_name, "Raw_Events_Log", 'append')

    print("\n================ ĐỒNG BỘ DỮ LIỆU HOÀN TẤT ================")

    # Đóng session
    spark.stop()


if __name__ == "__main__":
    run_batch_sync()