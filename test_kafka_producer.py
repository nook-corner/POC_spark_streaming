# -*- coding: utf-8 -*-
import pytest
from confluent_kafka import Consumer, KafkaException
import json
from collections import Counter
from datetime import datetime
import time
from test_config import KAFKA_BROKER, TOPIC

@pytest.fixture(scope="session")
def kafka_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "test-consumer-group",
        "auto.offset.reset": "earliest"
    })
    yield consumer
    consumer.close()

@pytest.fixture(scope="session")
def consumed_messages(kafka_consumer):
    kafka_consumer.subscribe([TOPIC])
    
   
    time.sleep(2)

    messages = []
    try:
        while True:
            msg = kafka_consumer.poll(3.0)
            if msg is None:
                break  
            if msg.error():
                raise KafkaException(msg.error())

            try:
                decoded_msg = json.loads(msg.value().decode("utf-8"))
                messages.append(decoded_msg)
            except json.JSONDecodeError:
                pytest.fail("Received malformed JSON from Kafka.")
    finally:
        kafka_consumer.close()

    return messages

def test_kafka_record_count(consumed_messages):
    assert len(consumed_messages) == 20, f"Expected 20 messages but got {len(consumed_messages)}"

def test_kafka_symbol_count(consumed_messages):
    symbol_counts = Counter(msg.get("symbol", "UNKNOWN") for msg in consumed_messages)
    missing_symbols = []

    for symbol in ["AAPL", "GOOGL", "TSLA"]:
        if symbol_counts.get(symbol, 0) != 4:
            missing_symbols.append(f"{symbol} has {symbol_counts.get(symbol, 0)} instead of 4")

    assert not missing_symbols, "Stock symbol count mismatch: " + ", ".join(missing_symbols)

def test_kafka_data_types(consumed_messages):
    for msg in consumed_messages:
        errors = []

        if not isinstance(msg.get("txn_id"), str):
            errors.append("txn_id should be a string")
        if not isinstance(msg.get("symbol"), str):
            errors.append("symbol should be a string")
        if not isinstance(msg.get("buyer"), str):
            errors.append("buyer should be a string")
        if not isinstance(msg.get("seller"), str):
            errors.append("seller should be a string")
        if "order_type" in msg and not isinstance(msg.get("order_type"), str):
            errors.append("order_type should be a string")

        if "timestamp" in msg:
            try:
                datetime.fromisoformat(msg["timestamp"])
            except (ValueError, TypeError):
                errors.append("timestamp is not in valid ISO format")

        if not isinstance(msg.get("price"), (float, int)):
            errors.append("price should be a float or int")
        if not isinstance(msg.get("volume"), int):
            errors.append("volume should be an int")

        assert not errors, "Invalid Data Types: " + ", ".join(errors)
