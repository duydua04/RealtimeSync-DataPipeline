import json
import time

from kafka import KafkaProducer
from databases.mysql_connect import MySQLConnect
from config.db_config import get_database_config

def get_data_trigger(mysql_client, last_timestamp):
    connection, cursor = mysql_client.connection, mysql_client.cursor
    database = 'github_data'
    connection.database = database

    query = """
        SELECT 
            log_id, user_id, login, avatar_url, url, type, site_admin,
            DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at,
            status,
            DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp
        FROM USERS_LOG_AFTER
    """

    if last_timestamp:
        query += ' WHERE log_timestamp > %s ORDER BY log_timestamp ASC'
        cursor.execute(query, (last_timestamp,))
    else:
        query += " ORDER BY log_timestamp ASC"
        cursor.execute(query)

    rows = cursor.fetchall()
    connection.commit()

    schema = ["log_id", "user_id", "login", "avatar_url", "url",
              "type", "site_admin", "created_at", "status", "log_timestamp"
    ]
    data = [dict(zip(schema, row)) for row in rows]

    max_timestamp = max((row['timestamp'] for row in data), default=last_timestamp) if data else last_timestamp

    return data, max_timestamp

def main():
    last_timestamp = None
    config = get_database_config()

    while True:
        try:
            with MySQLConnect(config["mysql"].host, config["mysql"].port,
                              config["mysql"].user, config["mysql"].password) as mysql_client:
                producer = KafkaProducer(
                    bootstrap_servers='localhost:9092',
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )

                while True:
                    data, max_timestamp = get_data_trigger(mysql_client, last_timestamp)
                    if data:
                        print(f"[{max_timestamp}] ⚡ Tìm thấy {len(data)} bản ghi mới. Đang đẩy lên Kafka...")
                        for record in data:
                            producer.send("top", record)

                        producer.flush()
                        last_timestamp = max_timestamp

                    time.sleep(2)
        except Exception as e:
            print(f"Lỗi kết nối hoặc thực thi: {e}. Đang thử lại sau 5 giây...")
            time.sleep(5)


if __name__== "__main__":
    main()