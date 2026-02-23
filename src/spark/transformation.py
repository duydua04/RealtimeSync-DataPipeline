from pyspark.sql.functions import col, lit, struct, to_timestamp, when
from pyspark.sql.types import *
from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_mysql import SparkWriteMySQL
from src.spark.spark_write_mongodb import SparkWriteToMongodb


def main():
    # 1. KHỞI TẠO SPARK
    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ]

    spark_conn = SparkConnect(
        app_name="Github_ETL_Master",
        master_url="local[*]",
        jar_packages=jars,
        log_level="WARN"
    )

    schema_full = StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("actor", StructType([
            StructField("id", LongType(), True),
            StructField("login", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ])),
        StructField("repo", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True)
        ])),
        StructField("org", StructType([
            StructField("id", LongType(), True),
            StructField("login", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ])),
        StructField("payload", StructType([
            StructField("forkee", StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("full_name", StringType(), True),
                StructField("owner", StructType([
                    StructField("id", LongType(), True),
                    StructField("login", StringType(), True),
                    StructField("type", StringType(), True)
                ])),
                StructField("private", BooleanType(), True),
                StructField("fork", BooleanType(), True),
                StructField("html_url", StringType(), True),
                StructField("description", StringType(), True),
                StructField("homepage", StringType(), True),
                StructField("size", IntegerType(), True),
                StructField("language", StringType(), True),
                StructField("forks_count", IntegerType(), True),
                StructField("stargazers_count", IntegerType(), True),
                StructField("watchers_count", IntegerType(), True),
                StructField("default_branch", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("pushed_at", StringType(), True)
            ])),
            StructField("ref", StringType(), True),
            StructField("head", StringType(), True)
        ]))
    ])

    json_path = "/home/hoangduy/PycharmProjects/DataPipeline/data/2015-03-01-17.json"
    df_raw = spark_conn.spark.read.schema(schema_full).json(json_path)

    df_raw = df_raw.withColumn("created_at_ts", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    print("Processing Transformations...")

    df_users_actor = df_raw.select(
        col("actor.id").alias("user_id"), col("actor.login").alias("login"),
        col("actor.avatar_url"), col("actor.url"), lit("User").alias("type"),
        lit(False).alias("site_admin"), col("created_at_ts").alias("created_at")
    ).filter(col("user_id").isNotNull())

    df_users_owner = df_raw.select(
        col("payload.forkee.owner.id").alias("user_id"), col("payload.forkee.owner.login").alias("login"),
        lit(None).cast(StringType()).alias("avatar_url"), lit(None).cast(StringType()).alias("url"),
        col("payload.forkee.owner.type").alias("type"), lit(False).alias("site_admin"),
        col("created_at_ts").alias("created_at")
    ).filter(col("user_id").isNotNull())

    df_mysql_users = df_users_actor.unionByName(df_users_owner, allowMissingColumns=True).dropDuplicates(["user_id"])

    df_mysql_orgs = df_raw.select(
        col("org.id").alias("org_id"), col("org.login").alias("login"),
        col("org.url"), col("org.avatar_url"), col("created_at_ts").alias("created_at")
    ).filter(col("org_id").isNotNull()).dropDuplicates(["org_id"])

    df_repo_base = df_raw.select(
        col("repo.id").alias("repo_id"), col("repo.name").alias("name"),
        lit(None).cast(StringType()).alias("full_name"), col("repo.url"),
        lit(None).cast(StringType()).alias("html_url"), lit(None).cast(StringType()).alias("description"),
        lit(False).alias("is_private"), lit(False).alias("is_fork"),
        lit(None).cast(StringType()).alias("homepage"), lit(0).alias("size"),
        lit(None).cast(StringType()).alias("language"), lit(0).alias("forks_count"),
        lit(0).alias("stargazers_count"), lit(0).alias("watchers_count"),
        lit(None).cast(StringType()).alias("default_branch"),
        lit(None).cast(TimestampType()).alias("created_at"),
        lit(None).cast(TimestampType()).alias("updated_at"),
        lit(None).cast(TimestampType()).alias("pushed_at")
    ).filter(col("repo_id").isNotNull())

    df_repo_forkee = df_raw.select(
        col("payload.forkee.id").alias("repo_id"), col("payload.forkee.name").alias("name"),
        col("payload.forkee.full_name").alias("full_name"), lit(None).cast(StringType()).alias("url"),
        col("payload.forkee.html_url").alias("html_url"), col("payload.forkee.description").alias("description"),
        col("payload.forkee.private").alias("is_private"), col("payload.forkee.fork").alias("is_fork"),
        col("payload.forkee.homepage").alias("homepage"), col("payload.forkee.size").alias("size"),
        col("payload.forkee.language").alias("language"), col("payload.forkee.forks_count").alias("forks_count"),
        col("payload.forkee.stargazers_count").alias("stargazers_count"),
        col("payload.forkee.watchers_count").alias("watchers_count"),
        col("payload.forkee.default_branch").alias("default_branch"),
        to_timestamp(col("payload.forkee.created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("created_at"),
        to_timestamp(col("payload.forkee.updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("updated_at"),
        to_timestamp(col("payload.forkee.pushed_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("pushed_at")
    ).filter(col("repo_id").isNotNull())

    df_mysql_repos = df_repo_base.unionByName(df_repo_forkee, allowMissingColumns=True).dropDuplicates(["repo_id"])

    df_mysql_events = df_raw.select(
        col("id").alias("event_id"), col("type").alias("event_type"),
        col("actor.id").alias("actor_id"), col("repo.id").alias("repo_id"),
        col("org.id").alias("org_id"), col("public").alias("is_public"),
        col("created_at_ts").alias("created_at"),
        when(col("type") == "ForkEvent", col("payload.forkee.id")).otherwise(lit(None)).alias("forkee_repo_id")
    ).dropDuplicates(["event_id"])

    df_ownership_org = df_raw.select(
        col("repo.id").alias("repo_id"), lit(None).cast(LongType()).alias("owner_user_id"),
        col("org.id").alias("owner_org_id")
    ).filter(col("repo_id").isNotNull() & col("owner_org_id").isNotNull())

    df_ownership_user = df_raw.select(
        col("payload.forkee.id").alias("repo_id"), col("payload.forkee.owner.id").alias("owner_user_id"),
        lit(None).cast(LongType()).alias("owner_org_id")
    ).filter(col("repo_id").isNotNull() & col("owner_user_id").isNotNull())

    df_mysql_ownership = df_ownership_org.unionByName(df_ownership_user).dropDuplicates(["repo_id"])

    df_mongo_users = df_mysql_users.withColumnRenamed("user_id", "_id")
    df_mongo_orgs = df_mysql_orgs.withColumnRenamed("org_id", "_id")

    df_mongo_repos = df_repo_forkee.alias("r").join(
        df_raw.select(col("payload.forkee.id").alias("fk_id"), col("payload.forkee.owner")).dropDuplicates(["fk_id"]),
        col("r.repo_id") == col("fk_id"), "left"
    ).select(
        col("repo_id").alias("_id"), col("name"), col("full_name"), col("url"), col("html_url"),
        col("description"), col("is_private"), col("is_fork"), col("language"), col("default_branch"),
        struct(col("size"), col("forks_count"), col("stargazers_count"), col("watchers_count")).alias("stats"),
        struct(col("created_at"), col("updated_at"), col("pushed_at")).alias("dates"),
        struct(col("owner.id"), col("owner.login"), col("owner.type")).alias("owner")
    ).filter(col("owner.id").isNotNull())

    # MONGODB: EVENTS (Gom nhóm actor, repo, org, payload)
    df_mongo_events = df_raw.select(
        col("id").alias("_id"), col("type"), col("public").alias("is_public"), col("created_at_ts").alias("created_at"),
        struct(col("actor.id"), col("actor.login"), col("actor.avatar_url")).alias("actor"),
        struct(col("repo.id"), col("repo.name"), col("repo.url")).alias("repo"),
        when(col("org.id").isNotNull(), struct(col("org.id"), col("org.login"))).otherwise(lit(None)).alias("org"),
        struct(
            struct(col("payload.forkee.id"), col("payload.forkee.name"), col("public")).alias("forkee"),
            col("payload.ref"), col("payload.head")
        ).alias("payload")
    )

    #--- LOAD TO DATABASE ---
    configs = get_spark_config()
    mysql_writer = SparkWriteMySQL(spark_conn.spark, configs["mysql"])
    mongo_writer = SparkWriteToMongodb(spark_conn.spark, configs["mongodb"])

    print("\n--- WRITE TO MYSQL ---")
    jdbc_url = configs["mysql"]["jdbc"]
    my_cfg = configs["mysql"]

    # 1. Bảng độc lập không chứa khóa ngoại
    mysql_writer.spark_write_mysql(df_mysql_users, my_cfg, jdbc_url, "USERS", "append")
    mysql_writer.spark_write_mysql(df_mysql_orgs, my_cfg, jdbc_url, "ORGS", "append")
    mysql_writer.spark_write_mysql(df_mysql_repos, my_cfg, jdbc_url, "REPOS", "append")

    # 2. Bảng phụ thuộc (Chứa Khóa ngoại trỏ về 3 bảng trên)
    mysql_writer.spark_write_mysql(df_mysql_events, my_cfg, jdbc_url, "EVENTS", "append")
    mysql_writer.spark_write_mysql(df_mysql_ownership, my_cfg, jdbc_url, "REPO_OWNERSHIP", "append")

    print("\n--- WRITE TO MONGODB ---")
    mongo_uri = configs["mongodb"]["uri"]
    mongo_db = configs["mongodb"]["database"]

    mongo_writer.sparkwritetomongodb(df_mongo_users, mongo_uri, mongo_db, "users", "append")
    mongo_writer.sparkwritetomongodb(df_mongo_orgs, mongo_uri, mongo_db, "orgs", "append")
    mongo_writer.sparkwritetomongodb(df_mongo_repos, mongo_uri, mongo_db, "repos", "append")
    mongo_writer.sparkwritetomongodb(df_mongo_events, mongo_uri, mongo_db, "events", "append")

    print("---- WRITE COMPLETE ----")
    spark_conn.stop()


if __name__ == "__main__":
    main()