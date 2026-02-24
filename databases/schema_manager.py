from mysql.connector import Error


def create_mysql_schema(connection, cursor):
    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    print(f"--------DROP database: {database} in MYSQL---------")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    print(f"--------CREATE database: {database} in MYSQL--------")
    connection.database = database

    try:
        with open("/home/hoangduy/PycharmProjects/DataPipeline/src/sql/schema.sql", 'r') as f:
            sql_script = f.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f'-------Executed Mysql Command: {cmd}--------')
            print("-----CREATED MYSQL SCHEMA------")
    except Error as e:
        connection.rollback()
        raise Exception(f"-------Failed to CREATE MYSQL SCHEMA: ERROR : {e}--------") from e

def create_mongo_schema(db):
    db.drop_collection("users")
    db.create_collection("users", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id", "login"],
            "properties": {
                "_id": {"bsonType": ["int", "long"], "description": "user_id từ GitHub"},
                "login": {"bsonType": "string"},
                "avatar_url": {"bsonType": ["string", "null"]},
                "url": {"bsonType": ["string", "null"]},
                "type": {"bsonType": ["string", "null"]},
                "site_admin": {"bsonType": ["bool", "null"]},
                "created_at": {"bsonType": ["date", "string", "null"]}
            }
        }
    })

    db.drop_collection("orgs")
    db.create_collection("orgs", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id"],
            "properties": {
                "_id": {"bsonType": ["long", "int"], "description": "org_id từ GitHub"},
                "login": {"bsonType": ["string", "null"]},
                "url": {"bsonType": ["string", "null"]},
                "avatar_url": {"bsonType": ["string", "null"]},
                "created_at": {"bsonType": ["date", "string", "null"]}
            }
        }
    })

    db.drop_collection("repos")
    db.create_collection("repos", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id"],
            "properties": {
                "_id": {"bsonType": ["long", "int"], "description": "repo_id từ GitHub"},
                "name": {"bsonType": ["string", "null"]},
                "full_name": {"bsonType": ["string", "null"]},
                "url": {"bsonType": ["string", "null"]},
                "html_url": {"bsonType": ["string", "null"]},
                "description": {"bsonType": ["string", "null"]},
                "is_private": {"bsonType": ["bool", "null"]},
                "is_fork": {"bsonType": ["bool", "null"]},
                "stats": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "size": {"bsonType": ["int", "long", "null"]},
                        "forks_count": {"bsonType": ["int", "long", "null"]},
                        "stargazers_count": {"bsonType": ["int", "long", "null"]},
                        "watchers_count": {"bsonType": ["int", "long", "null"]}
                    }
                },
                "language": {"bsonType": ["string", "null"]},
                "default_branch": {"bsonType": ["string", "null"]},
                "dates": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "created_at": {"bsonType": ["date", "string", "null"]},
                        "updated_at": {"bsonType": ["date", "string", "null"]},
                        "pushed_at": {"bsonType": ["date", "string", "null"]}
                    }
                },
                "owner": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "id": {"bsonType": ["long", "int", "null"]},
                        "login": {"bsonType": ["string", "null"]},
                        "type": {"bsonType": ["string", "null"]}
                    }
                }
            }
        }
    })

    db.drop_collection("events")
    db.create_collection("events", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id", "type"],
            "properties": {
                "_id": {"bsonType": "string", "description": "event_id"},
                "type": {"bsonType": ["string", "null"]},
                "is_public": {"bsonType": ["bool", "null"]},
                "created_at": {"bsonType": ["date", "string", "null"]},
                "actor": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "id": {"bsonType": ["long", "int", "null"]},
                        "login": {"bsonType": ["string", "null"]},
                        "avatar_url": {"bsonType": ["string", "null"]}
                    }
                },
                "repo": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "id": {"bsonType": ["long", "int", "null"]},
                        "name": {"bsonType": ["string", "null"]},
                        "url": {"bsonType": ["string", "null"]}
                    }
                },
                "org": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "id": {"bsonType": ["long", "int", "null"]},
                        "login": {"bsonType": ["string", "null"]}
                    }
                },
                "payload": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "forkee": {
                            "bsonType": ["object", "null"],
                            "properties": {
                                "id": {"bsonType": ["long", "int", "null"]},
                                "name": {"bsonType": ["string", "null"]},
                                "public": {"bsonType": ["bool", "null"]}
                            }
                        },
                        "ref": {"bsonType": ["string", "null"]},
                        "head": {"bsonType": ["string", "null"]}
                    }
                }
            }
        }
    })

    db.users.create_index("login", unique=True)
    db.repos.create_index("full_name")
    db.repos.create_index("owner.id")
    db.events.create_index("actor.id")
    db.events.create_index("repo.id")
    print("----------_CREATED MONGODB SCHEMA-------------")

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    table_list = cursor.fetchall()
    tables = [row[0] for row in table_list]

    required_tables = ["users", "repos", "orgs", "events", "repo_ownership"]

    for table in required_tables:
        if table not in tables:
            raise ValueError(f"========= Table '{table}' isn't exist =============")

    print("-----------MySQL: TABLES IS CREATED FULLY.--------")

    cursor.execute("SELECT * FROM users WHERE user_id = 1")
    user = cursor.fetchall()
    if not user:
        raise ValueError("---------------MySQL: Test data (user_id=1) has been inserted complete!--------")

    print("-----------MySQL: Validated Schema & data test --------")


def validate_mongodb_schema(db):
    """Kiểm tra xem các collections trong MongoDB đã được tạo thành công chưa và có data không."""
    collections = db.list_collection_names()

    required_collections = ["users", "repos", "orgs", "events"]

    for coll in required_collections:
        if coll not in collections:
            raise ValueError(f"------MongoDB: Collection '{coll}' doesn't exist!-----")

    print("-----------MongoDB: Collections is created complete.--------")

    user = db.users.find_one({"_id": 1})

    if not user:
        raise ValueError("----------MongoDB: Test data (_id=1) insert incomplete!----------")

    print("--------------MongoDB: Validated Schema & test data!--------")


def create_mysql_triggers(connection, cursor):
    """Hàm thông minh tự động đọc file trigger.sql và thực thi"""
    # Đảm bảo đường dẫn này trỏ đúng tới file trigger.sql của bạn
    trigger_file = "/home/hoangduy/PycharmProjects/DataPipeline/src/sql/trigger.sql"

    try:
        print(f"\n🚀 BẮT ĐẦU TẠO CDC LOG TABLES & TRIGGERS TỪ FILE SQL...")

        # Dọn dẹp đường truyền trước khi bắt đầu
        while connection.unread_result:
            cursor.fetchall()

        with open(trigger_file, 'r') as f:
            sql_script = f.read()

        # TÁCH FILE THÀNH 2 PHẦN DỰA TRÊN TỪ KHÓA "DELIMITER //"
        parts = sql_script.split("DELIMITER //")

        commands = []

        # PHẦN 1: TẠO BẢNG (Trước DELIMITER //) -> Cắt bằng dấu chấm phẩy (;)
        if len(parts) > 0:
            table_queries = [cmd.strip() for cmd in parts[0].split(";") if cmd.strip()]
            commands.extend(table_queries)

        # PHẦN 2: TẠO TRIGGER (Sau DELIMITER //) -> Cắt bằng dấu (//)
        if len(parts) > 1:
            # Xóa bỏ nốt từ khóa DELIMITER ; ở cuối file
            trigger_part = parts[1].replace("DELIMITER ;", "")
            trigger_queries = [cmd.strip() for cmd in trigger_part.split("//") if cmd.strip()]
            commands.extend(trigger_queries)

        # THỰC THI TỪNG LỆNH ĐÃ ĐƯỢC CẮT CHUẨN XÁC
        for i, cmd in enumerate(commands):
            if not cmd:
                continue
            try:
                cursor.execute(cmd)

                # Bắt buộc phải dọn sạch bộ nhớ đệm sau mỗi lệnh (Chống lỗi Out of sync)
                while connection.unread_result:
                    cursor.fetchall()

                print(f"✅ Executed command {i + 1}/{len(commands)}")
            except Error as cmd_err:
                # Bỏ qua lỗi nếu bảng hoặc trigger đã tồn tại từ trước
                if "already exists" not in str(cmd_err).lower():
                    print(f"❌ Lỗi Cú Pháp tại lệnh số {i + 1}:\n{cmd[:150]}...")
                    raise cmd_err

        connection.commit()
        print("🎉 ----- TẤT CẢ TRIGGERS VÀ LOG TABLES ĐÃ SẴN SÀNG! ------")

    except Exception as e:
        connection.rollback()
        raise Exception(f"❌ Lỗi khi đọc file trigger.sql: {e}") from e