from kafka import KafkaConsumer, KafkaProducer
import json
import hashlib


def validate_record(record):
    # Extract the hash and message_id from the received record
    received_hash = record.pop("hash", None)
    message_id = record.get("message_id", "unknown")

    # Check if hash or message_id is missing
    if not received_hash:
        print(f"Validation failed for message_id {message_id}: No hash found")
        return False, message_id, "No hash found"
    if message_id == "unknown":
        print(f"Validation failed for message_id {message_id}: No message_id found")
        return False, message_id, "No message_id found"

    # Recompute the hash of the received record (excluding the hash field)
    computed_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode('utf-8')).hexdigest()

    # Compare hashes
    if computed_hash == received_hash:
        print(f"Validation passed for message_id {message_id}")
        return True, message_id, None
    else:
        print(f"Validation failed for message_id {message_id}: Hash mismatch")
        return False, message_id, f"Hash mismatch (received: {received_hash}, computed: {computed_hash})"


def consume_and_validate():
    # Initialize consumer
    consumer = KafkaConsumer(
        "vnontop",
        bootstrap_servers="192.168.2.26:9092",
        auto_offset_reset="earliest",  # Start from the beginning for testing
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Initialize producer for dead-letter queue
    dlq_producer = KafkaProducer(
        bootstrap_servers="192.168.2.26:9092",
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    # Initialize producer for Spark input topic
    spark_producer = KafkaProducer(
        bootstrap_servers="192.168.2.26:9092",
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    for message in consumer:
        record = message.value
        print(f"Received record: {record}")

        # Validate the record
        is_valid, message_id, error_reason = validate_record(record.copy())  # Use copy to avoid modifying original

        if is_valid:
            # Send valid record to Spark input topic
            spark_producer.send("spark_input", record)
            spark_producer.flush()
            print(f"Sent valid record to spark_input topic: message_id {message_id}")
        else:
            # Send invalid record to dead-letter queue
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
            dlq_producer.flush()
            print(f"Sent invalid record to dead-letter queue: {invalid_record}")


if __name__ == "__main__":
    consume_and_validate()