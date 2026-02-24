from kafka import KafkaProducer
from databases.mysql_connect import MySQLConnect
from config.db_config import get_database_config
import json
import time
from datetime import datetime


def get_data_trigger(mysql_client, last_timestamp):
    connection, cursor = mysql_client.connection, mysql_client.cursor
    database = "github_data"
    connection.database = database

    base_query = (
        "SELECT user_id, login, gravatar_id, url, avatar_url, status, "
        "log_timestamp "
        "FROM USER_LOG_AFTER"
    )

    if last_timestamp is not None and last_timestamp != "":
        query = base_query + " WHERE log_timestamp > %s ORDER BY log_timestamp ASC"
        cursor.execute(query, (last_timestamp,))
    else:
        query = base_query + " ORDER BY log_timestamp ASC"
        cursor.execute(query)

    rows = cursor.fetchall()
    connection.commit()

    schema = ["user_id", "login", "gravatar_id", "url", "avatar_url", "status", "log_timestamp"]
    data = [dict(zip(schema, row)) for row in rows]

    # Process each record with proper timestamp formatting
    for record in data:
        if hasattr(record["log_timestamp"], 'strftime'):
            record["formatted_timestamp"] = record["log_timestamp"].strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            record["formatted_timestamp"] = str(record["log_timestamp"])

    max_timestamp = max((row["log_timestamp"] for row in data), default=last_timestamp) if data else last_timestamp
    return data, max_timestamp


def main():
    last_timestamp = None
    total_records_sent = 0
    batch_id = 1
    config = get_database_config()

    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                      config["mysql"].password) as mysql_client:
        producer = KafkaProducer(
            bootstrap_servers="192.168.2.26:9092",
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            batch_size=16384,
            linger_ms=10
        )

        print("Producer started...")

        while True:
            try:
                data, max_timestamp = get_data_trigger(mysql_client, last_timestamp)
                batch_records = 0

                if data:
                    print(f"Processing batch {batch_id} with {len(data)} records")

                for record in data:
                    total_records_sent += 1
                    batch_records += 1

                    # Create enriched record with metadata
                    enriched_record = {
                        "user_id": record["user_id"],
                        "login": record["login"],
                        "gravatar_id": record.get("gravatar_id"),
                        "url": record.get("url"),
                        "avatar_url": record.get("avatar_url"),
                        "status": record["status"],
                        "log_timestamp": record["formatted_timestamp"],
                        "count_number": total_records_sent,
                        "batch_id": batch_id,
                        "producer_timestamp": datetime.now().isoformat(),
                        "record_version": "1.0"
                    }

                    producer.send("hoangduy", enriched_record)
                    print(f"Sent record {total_records_sent}: user_id={record['user_id']}, status={record['status']}")

                producer.flush()

                if isinstance(max_timestamp, str):
                    last_ts_str = max_timestamp
                elif hasattr(max_timestamp, 'isoformat'):
                    last_ts_str = max_timestamp.isoformat(sep=' ')
                else:
                    last_ts_str = str(max_timestamp)

                print(f"BATCH {batch_id} SUMMARY:")
                print(f"Last timestamp: {last_ts_str}")
                print(f"Records in batch: {batch_records}")
                print(f"Total records sent: {total_records_sent}")

                if batch_records > 0:
                    print(f"Successfully sent {batch_records} records")
                    batch_id += 1
                else:
                    print(f"   💤 No new records to send")

                last_timestamp = max_timestamp
                time.sleep(1)

            except KeyboardInterrupt:
                print(f"Producer stopped by user")
                break
            except Exception as e:
                print(f"Error in producer: {e}")
                time.sleep(5)


if __name__ == "__main__":
    main()