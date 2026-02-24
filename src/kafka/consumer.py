import os
import json
import hashlib
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()


def validate_record(record):
    # Trích xuất hash, message_id và table_name
    received_hash = record.pop("hash", None)
    message_id = record.get("message_id", "unknown")
    table_name = record.get("table_name")

    if not received_hash:
        return False, message_id, "No hash found"
    if not table_name:
        return False, message_id, "Missing table_name (Routing Label)"
    if message_id == "unknown":
        return False, message_id, "No message_id found"

    computed_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode('utf-8')).hexdigest()

    if computed_hash == received_hash:
        print(f"OK: {table_name.upper()} | ID: {message_id}")
        return True, message_id, None
    else:
        print(f"HASH ERROR - ID: {message_id}")
        return False, message_id, f"Hash mismatch (received: {received_hash}, computed: {computed_hash})"


def consume_and_validate():
    KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "192.168.2.26:9092")
    INPUT_TOPIC = "hoangduy"

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    spark_producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print(f"🎧 Consumer is listening data from topic '{INPUT_TOPIC}'...")

    try:
        for message in consumer:
            record = message.value

            is_valid, message_id, error_reason = validate_record(record.copy())

            if is_valid:
                spark_producer.send("spark_input", record)
            else:
                invalid_record = {
                    "original_record": record,
                    "error_reason": error_reason,
                    "message_id": message_id,
                    "timestamp": message.timestamp,
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset
                }
                dlq_producer.send("hoangduy_dead_letter_queue", invalid_record)

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        spark_producer.flush()
        dlq_producer.flush()


if __name__ == "__main__":
    consume_and_validate()