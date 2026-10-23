
import json
import os
import time
import urllib.error
import urllib.request

BASE_URL = "http://kafka-connect:8083"
CONNECTOR_NAME = "zibal-postgres-cdc"

config = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "zibal",
    "database.password": os.environ["POSTGRES_PASSWORD"],
    "database.dbname": "payment_db",

    "topic.prefix": "zibal",
    "plugin.name": "pgoutput",
    "slot.name": "zibal_cdc_slot",

    "publication.name": "zibal_publication",
    "publication.autocreate.mode": "filtered",

    "table.include.list": "core.merchants,core.transactions",

    "snapshot.mode": "initial",
    "snapshot.fetch.size": "10000",

    "tombstones.on.delete": "false",
    "heartbeat.interval.ms": "10000",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
}

def request(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.read().decode()


for attempt in range(60):
    try:
        request("GET", "/connector-plugins")
        break
    except (urllib.error.URLError, TimeoutError):
        print("Waiting for Kafka Connect...", flush=True)
        time.sleep(3)
else:
    raise RuntimeError("Kafka Connect did not become ready")

try:
    request("GET", f"/connectors/{CONNECTOR_NAME}")
    print("Connector already exists; preserving its configuration.")
except urllib.error.HTTPError as error:
    if error.code != 404:
        raise

    request(
        "POST",
        "/connectors",
        {"name": CONNECTOR_NAME, "config": config},
    )
    print("CDC connector created successfully.")
