from databases.mongodb_connect import MongoDBConnect
from databases.mysql_connect import MySQLConnect
from config.db_config import get_database_config
from databases.schema_manager import create_mysql_schema, create_mongo_schema, create_mysql_triggers


def main(config):
    config_mysql = config["mysql"]
    with MySQLConnect(config_mysql.host, config_mysql.port, config_mysql.user, config_mysql.password) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(connection, cursor)
        create_mysql_triggers(connection, cursor)

    config_mongodb = config["mongodb"]
    with MongoDBConnect(config_mongodb.uri, config_mongodb.database) as mongo_client:
        create_mongo_schema(db=mongo_client.connect())

if __name__ == "__main__":
    config = get_database_config()
    main(config)