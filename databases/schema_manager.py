# Dung python tao databas   e voi mysql, mongodb
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
        connection.roolback()
        raise Exception(f"-------Failed to CREATE MYSQL SCHEMA: ERROR : {e}--------") from e


