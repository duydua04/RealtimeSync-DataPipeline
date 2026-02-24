from kafka import KafkaProducer
from databases.mysql_connect import MySQLConnect
from config.db_config import get_database_config
import json
import time
import hashlib
from datetime import datetime

TABLE_CONFIGS = {
    "users": ["user_id", "login", "avatar_url", "url", "type", "site_admin", "created_at", "status", "log_timestamp"],
    "repos": ["repo_id", "name", "full_name", "url", "html_url", "description", "is_private", "is_fork", "homepage",
              "size", "language", "forks_count", "stargazers_count", "watchers_count", "default_branch", "created_at",
              "updated_at", "pushed_at", "status", "log_timestamp"],
    "orgs": ["org_id", "login", "url", "avatar_url", "created_at", "status", "log_timestamp"],
    "events": ["event_id", "event_type", "actor_id", "repo_id", "org_id", "is_public", "created_at", "forkee_repo_id",
               "status", "log_timestamp"],
    "repo_ownership": ["repo_id", "owner_user_id", "owner_org_id", "status", "log_timestamp"]
}


def get_data_trigger(mysql_client, table_name, last_timestamp):
    connection, cursor = mysql_client.connection, mysql_client.cursor
    connection.database = "github_data"

    columns = TABLE_CONFIGS[table_name]
    col_str = ", ".join(columns)
    log_table_name = f"{table_name.upper()}_LOG_AFTER"

    base_query = f"SELECT {col_str} FROM {log_table_name}"

    if last_timestamp is not None and last_timestamp != "":
        query = base_query + " WHERE log_timestamp > %s ORDER BY log_timestamp ASC"
        cursor.execute(query, (last_timestamp,))
    else:
        query = base_query + " ORDER BY log_timestamp ASC"
        cursor.execute(query)

    rows = cursor.fetchall()
    connection.commit()

    data = [dict(zip(columns, row)) for row in rows]

    for record in data:
        for key, val in record.items():
            if hasattr(val, 'strftime'):
                record[key] = val.strftime('%Y-%m-%d %H:%M:%S.%f') if 'timestamp' in key else val.strftime(
                    '%Y-%m-%d %H:%M:%S')

    max_ts = max((row["log_timestamp"] for row in data), default=last_timestamp) if data else last_timestamp
    return data, max_ts


def main():
    last_timestamps = {tbl: None for tbl in TABLE_CONFIGS.keys()}
    total_records_sent = 0
    batch_id = 1
    config = get_database_config()

    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                      config["mysql"].password) as mysql_client:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            batch_size=16384,
            linger_ms=10
        )

        print("---- Starting Producer ----")

        while True:
            try:
                has_new_data = False

                for table_name in TABLE_CONFIGS.keys():
                    data, max_timestamp = get_data_trigger(mysql_client, table_name, last_timestamps[table_name])

                    if data:
                        has_new_data = True
                        print(f"Processing batch {batch_id} with {len(data)} records")

                        for record in data:
                            total_records_sent += 1

                            enriched_record = record.copy()
                            enriched_record["table_name"] = table_name

                            record_id = record.get("user_id") or record.get("repo_id") or record.get(
                                "org_id") or record.get("event_id") or "unknown"
                            enriched_record["message_id"] = f"{table_name}_{record_id}_{record.get('log_timestamp')}"
                            enriched_record["producer_timestamp"] = datetime.now().isoformat()

                            hash_input = json.dumps(enriched_record, sort_keys=True).encode('utf-8')
                            enriched_record["hash"] = hashlib.sha256(hash_input).hexdigest()

                            producer.send("hoangduy", enriched_record)

                        producer.flush()

                        last_timestamps[table_name] = max_timestamp
                        batch_id += 1

                if not has_new_data:
                    pass

                time.sleep(1)

            except KeyboardInterrupt:
                print(f"Producer stopped by user")
                break
            except Exception as e:
                print(f"Error in producer: {e}")
                time.sleep(5)


if __name__ == "__main__":
    main()