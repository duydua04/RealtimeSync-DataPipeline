from databases.mysql_connect import MySQLConnect
from config.db_config import get_database_config
from databases.schema_manager import create_mysql_schema

def main(config):
    config_mysql = config["mysql"]
    with MySQLConnect(config_mysql.host, config_mysql.port, config_mysql.user, config_mysql.password) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(connection, cursor)

if __name__ == "__main__":
    config = get_database_config()
    main(config)