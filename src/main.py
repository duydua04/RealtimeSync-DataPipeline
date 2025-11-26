from databases.mysql_connect import MySQLConnect
from config.db_config import get_database_config

def main(config):
    MySQLConnect(
        config["mysql"].host,
        config["mysql"].port,
        config["mysql"].user,
        config["mysql"].password
    )

if __name__ == "__main__":
    config = get_database_config()
    main(config)