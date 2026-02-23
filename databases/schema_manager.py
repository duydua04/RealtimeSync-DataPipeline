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