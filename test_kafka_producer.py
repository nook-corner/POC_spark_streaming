import pytest
from confluent_kafka import Consumer
import json
from collections import Counter
from datetime import datetime
from test_config import KAFKA_BROKER, TOPIC

@pytest.fixture
def kafka_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "test-consumer-group",
        "auto.offset.reset": "earliest"
    })

def test_kafka_record_count(kafka_consumer):
    kafka_consumer.subscribe([TOPIC])
    messages = []
    for _ in range(20): 
        msg = kafka_consumer.poll(5.0)
        if msg is not None:
            messages.append(json.loads(msg.value().decode("utf-8")))
    
    kafka_consumer.close()
    assert len(messages) == 20, f"อ่านข้อความได้แค่ {len(messages)} ข้อความ"

def test_kafka_symbol_count(kafka_consumer):
    kafka_consumer.subscribe([TOPIC])
    messages = []
    for _ in range(20): 
        msg = kafka_consumer.poll(5.0)
        if msg is not None:
            messages.append(json.loads(msg.value().decode("utf-8")))
    
    kafka_consumer.close()
    
    symbol_counts = Counter(msg["symbol"] for msg in messages)
    missing_symbols = []
    
    for symbol in ["AAPL", "GOOGL", "TSLA"]:
        if symbol_counts.get(symbol, 0) != 4:
            missing_symbols.append(f"{symbol} {symbol_counts.get(symbol, 0)}")
    
    assert not missing_symbols, "จำนวนหุ้นไม่ถูกต้อง: " + ", ".join(missing_symbols)

def test_kafka_data_types(kafka_consumer):
    kafka_consumer.subscribe([TOPIC])
    messages = []
    for _ in range(20): 
        msg = kafka_consumer.poll(5.0)
        if msg is not None:
            messages.append(json.loads(msg.value().decode("utf-8")))
    
    kafka_consumer.close()
    
    for msg in messages:
        errors = []
        if not isinstance(msg["txn_id"], str):
            errors.append("txn_id ควรเป็น string")
        if not isinstance(msg["symbol"], str):
            errors.append("symbol ควรเป็น string")
        if not isinstance(msg["buyer"], str):
            errors.append("buyer ควรเป็น string")
        if not isinstance(msg["seller"], str):
            errors.append("seller ควรเป็น string")
        if "order_type" in msg and not isinstance(msg["order_type"], str):
            errors.append("order_type ควรเป็น string")
        
        try:
            datetime.fromisoformat(msg["timestamp"])
        except ValueError:
            errors.append("timestamp ไม่อยู่ในรูปแบบที่ถูกต้อง (ISO format)")
        
        if not isinstance(msg["price"], (float, int)):
            errors.append("price ควรเป็น float หรือ double")
        if not isinstance(msg["volume"], int):
            errors.append("volume ควรเป็น int")
        
        assert not errors, "Data Type ไม่ถูกต้อง: " + ", ".join(errors)
