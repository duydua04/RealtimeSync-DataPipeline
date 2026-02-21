from pyspark.sql.types import *
from pyspark.sql.functions import col, explode, to_timestamp, lit
from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_mysql import SparkWriteMySQL


def test_spark():
    jars = ["mysql:mysql-connector-java:8.0.33", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"]
    spark_connect = SparkConnect(
        app_name='spark_test',
        master_url='local[*]',
        executor_cores=3,
        executor_memory='2g',
        driver_memory='1g',
        executor_nums=1,
        jar_packages=jars,
        spark_conf={"spark.mongodb.write.connection.uri": "mongodb://admin:secret123@localhost:27019"},
        log_level='ERROR'
    )
    return spark_connect

def run_etl_process():
    spark_wrapper = test_spark()
    spark = spark_wrapper.spark
    mysql_config = get_spark_config()

    jdbc_url = mysql_config['mysql']['jdbc']
    db_properties = {
        "user": mysql_config['mysql']['user'],
        "password": mysql_config['mysql']['password'],
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df_raw = spark.read.option("multiLine", "true").json("../data/small_json.json")

    df = df_raw.withColumn("event_id_long", col("id").cast(LongType())) \
        .withColumn("created_at_ts", to_timestamp(col("created_at")))

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


    def extract_user(dataframe, prefix):
        def safe_col(p, cast_type, alias=None):
            alias = alias or p.split(".")[-1]
            if has_nested_field(dataframe, p):
                return col(p).cast(cast_type).alias(alias)
            return lit(None).cast(cast_type).alias(alias)

        return dataframe.select(
            safe_col(f"{prefix}.id", LongType(), "user_id"),
            safe_col(f"{prefix}.login", StringType(), "login"),
            safe_col(f"{prefix}.avatar_url", StringType(), "avatar_url"),
            safe_col(f"{prefix}.url", StringType(), "url"),
            safe_col(f"{prefix}.html_url", StringType(), "html_url"),
            safe_col(f"{prefix}.type", StringType(), "type"),
            safe_col(f"{prefix}.site_admin", BooleanType(), "site_admin")
        ).filter(col("user_id").isNotNull())

    df_users = extract_user(df, "actor") \
        .unionByName(extract_user(df, "payload.issue.user"), allowMissingColumns=True) \
        .unionByName(extract_user(df, "payload.comment.user"), allowMissingColumns=True) \
        .dropDuplicates(["user_id"])

    df_repos = df.select(
        col("repo.id").cast(LongType()).alias("repo_id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url")
    ).dropDuplicates(["repo_id"])

    if has_nested_field(df, "payload.issue"):
        df_issues = df.select(
            col("payload.issue.id").cast(LongType()).alias("issue_id"),
            col("repo.id").cast(LongType()).alias("repo_id"),
            col("payload.issue.user.id").cast(LongType()).alias("user_id"),
            col("payload.issue.number").alias("number"),
            col("payload.issue.title").alias("title"),
            col("payload.issue.body").alias("body"),
            col("payload.issue.state").alias("state"),
            col("payload.issue.locked").cast(BooleanType()).alias("locked"),

            col("payload.issue.comments").alias("comments_count"),
            to_timestamp(col("payload.issue.created_at")).alias("created_at"),
            to_timestamp(col("payload.issue.updated_at")).alias("updated_at"),
            to_timestamp(col("payload.issue.closed_at")).alias("closed_at")
        ).filter(col("issue_id").isNotNull()).dropDuplicates(["issue_id"])
    else:
        empty_schema = StructType() \
            .add("issue_id", LongType()).add("repo_id", LongType()).add("user_id", LongType()) \
            .add("number", LongType()).add("title", StringType()).add("body", StringType()) \
            .add("state", StringType()).add("locked", BooleanType()).add("comments_count", LongType()) \
            .add("created_at", StringType()).add("updated_at", StringType()).add("closed_at", StringType())
        df_issues = spark.createDataFrame([], empty_schema)

    if has_nested_field(df, "payload.comment"):
        df_comments = df.select(
            col("payload.comment.id").cast(LongType()).alias("comment_id"),
            col("payload.issue.id").cast(LongType()).alias("issue_id"),
            col("payload.comment.user.id").cast(LongType()).alias("user_id"),
            col("payload.comment.body").alias("body"),
            to_timestamp(col("payload.comment.created_at")).alias("created_at"),
            to_timestamp(col("payload.comment.updated_at")).alias("updated_at")
        ).filter(col("comment_id").isNotNull()).dropDuplicates(["comment_id"])
    else:
        df_comments = spark.createDataFrame([], StructType([]))

    df_events = df.select(
        col("id").cast(LongType()).alias("event_id"),
        col("type").alias("event_type"),
        col("actor.id").cast(LongType()).alias("actor_id"),
        col("repo.id").cast(LongType()).alias("repo_id"),
        col("public").cast(BooleanType()).alias("public"),

        col("created_at_ts").alias("created_at")
    ).dropDuplicates(["event_id"])

    df_payloads = df.select(
        col("id").cast(LongType()).alias("event_id"),
        col("payload.action").alias("action"),
        col("payload.issue.id").cast(LongType()).alias("issue_id"),
        col("payload.comment.id").cast(LongType()).alias("comment_id")
    ).filter(col("event_id").isNotNull()).dropDuplicates(["event_id"])

    df_labels = spark.createDataFrame([], StructType([]))

    if has_nested_field(df, "payload.issue.labels"):
        df_exploded = df.select(
            col("payload.issue.id").cast(LongType()).alias("issue_id"),
            explode(col("payload.issue.labels")).alias("lbl")
        ).filter(col("issue_id").isNotNull())

        df_labels = df_exploded.select(
            col("lbl.name").alias("name"),
            col("lbl.color").alias("color"),
            col("lbl.url").alias("url")
        ).dropDuplicates(["name"])

    writer = SparkWriteMySQL(spark, mysql_config)

    # 1. Ghi Users
    if df_users.count() > 0:
        print("Đang ghi Users...")
        writer.sparkwritetomysql(df_users, db_properties['user'], db_properties['password'], jdbc_url, "users",
                                 'append')

    # 2. Ghi Repositories
    if df_repos.count() > 0:
        print("Đang ghi Repositories...")
        writer.sparkwritetomysql(df_repos, db_properties['user'], db_properties['password'], jdbc_url, "repositories",
                                 'append')

    # 3. Ghi Labels (Để sinh ID tự tăng trong MySQL)
    if df_labels.count() > 0:
        print("Đang ghi Labels...")
        writer.sparkwritetomysql(df_labels, db_properties['user'], db_properties['password'], jdbc_url, "labels",
                                 'append')

    # 4. Ghi Issues (Cần Users và Repos có trước)
    if df_issues.count() > 0:
        print("Đang ghi Issues...")
        # Lỗi cũ của bạn nằm ở đây, giờ đã fix bằng .alias("locked") ở trên
        writer.sparkwritetomysql(df_issues, db_properties['user'], db_properties['password'], jdbc_url, "issues",
                                 'append')

    # 5. Ghi Issue_Labels (Logic phức tạp: Phải lấy ID từ MySQL ra)
    if df_labels.count() > 0 and has_nested_field(df, "payload.issue.labels"):
        print("Đang xử lý Issue_Labels...")
        # Đọc lại bảng labels từ MySQL để lấy label_id (vì nó là SERIAL/AUTO_INCREMENT)
        df_mysql_labels = spark.read.jdbc(url=jdbc_url, table="labels", properties=db_properties)

        # Join tên label từ file json với bảng label trong db để lấy ID
        df_issue_labels = df_exploded.select(
            col("issue_id"),
            col("lbl.name").alias("name")
        ).join(df_mysql_labels.select("label_id", "name"), on="name") \
            .select("issue_id", "label_id") \
            .dropDuplicates()

        if df_issue_labels.count() > 0:
            writer.sparkwritetomysql(df_issue_labels, db_properties['user'], db_properties['password'], jdbc_url,
                                     "issue_labels", 'append')

    # 6. Ghi Comments
    if df_comments.count() > 0:
        print("Đang ghi Comments...")
        writer.sparkwritetomysql(df_comments, db_properties['user'], db_properties['password'], jdbc_url, "comments",
                                 'append')

    # 7. Ghi Events
    if df_events.count() > 0:
        print("Đang ghi Events...")
        writer.sparkwritetomysql(df_events, db_properties['user'], db_properties['password'], jdbc_url, "events",
                                 'append')

    # 8. Ghi Payloads
    if df_payloads.count() > 0:
        print("Đang ghi Payloads...")
        writer.sparkwritetomysql(df_payloads, db_properties['user'], db_properties['password'], jdbc_url, "payloads",
                                 'append')

    print("--- Hoàn thành toàn bộ quy trình ETL ---")