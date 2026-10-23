
import json
import os
import time

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException
from bootstrap_clickhouse import bootstrap


BATCH_SIZE = 5000

TOPICS = [
    "zibal.core.merchants",
    "zibal.core.transactions",
]

COLUMNS = [
    "topic",
    "partition_id",
    "kafka_offset",
    "operation",
    "entity_id",
    "source_lsn",
    "event_json",
]


def connect_clickhouse():
    while True:
        try:
            client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                username="zibal",
                password=os.environ["CLICKHOUSE_PASSWORD"],
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:
            print(f"Waiting for ClickHouse: {exc}")
            time.sleep(5)


def create_consumer():
    return Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "zibal-clickhouse-raw-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 600000,
    })


def parse_message(msg):
    raw_value = msg.value()

    # Debezium tombstone messages have a null value.
    if raw_value is None:
        return None

    payload = json.loads(raw_value.decode("utf-8"))

    if payload is None:
        return None

    operation = payload.get("op")
    source = payload.get("source") or {}

    record = payload.get("after") or payload.get("before") or {}

    if msg.topic().endswith(".transactions"):
        entity_id = record.get("transaction_id")
    else:
        entity_id = record.get("merchant_id")

    if entity_id is None:
        raise ValueError(
            f"Missing primary key at "
            f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        )

    return [
        msg.topic(),
        msg.partition(),
        msg.offset(),
        operation or "",
        int(entity_id),
        int(source.get("lsn") or 0),
        raw_value.decode("utf-8"),
    ]


def flush_batch(client, consumer, rows, messages):
    if not messages:
        return

    if rows:
        client.insert(
            "raw.cdc_events",
            rows,
            column_names=COLUMNS,
        )

    # Store only offsets whose messages were inserted.
    # For each partition, store the latest processed message.
    latest = {}

    for msg in messages:
        latest[(msg.topic(), msg.partition())] = msg

    for msg in latest.values():
        consumer.store_offsets(message=msg)

    # Commit only after ClickHouse accepted the batch.
    consumer.commit(asynchronous=False)

    print(
        f"Processed {len(messages):,} Kafka messages; "
        f"inserted {len(rows):,} CDC events",
        flush=True,
    )


def main():
    client = connect_clickhouse()
    bootstrap(client)
    consumer = create_consumer()

    consumer.subscribe(TOPICS)

    rows = []
    messages = []
    last_flush = time.monotonic()

    print("CDC consumer started", flush=True)

    try:
        while True:
            msg = consumer.poll(0.5)

            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())

                row = parse_message(msg)

                if row is not None:
                    rows.append(row)

                messages.append(msg)

            should_flush = (
                len(messages) >= BATCH_SIZE
                or (
                    messages
                    and time.monotonic() - last_flush >= 2
                )
            )

            if should_flush:
                flush_batch(
                    client, consumer, rows, messages
                )
                rows.clear()
                messages.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        print("Stopping consumer...", flush=True)

    finally:
        if messages:
            flush_batch(client, consumer, rows, messages)

        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
