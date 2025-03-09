from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import json
import random
import time
from faker import Faker

fake = Faker()

# Kafka Configuration
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf) 

# Avro Schema
avro_schema_str = """
{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "txn_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "volume", "type": "int"},
    {"name": "buyer", "type": "string"},
    {"name": "seller", "type": "string"}
  ]
}
"""

# Create Avro Serializer
avro_serializer = AvroSerializer(
    schema_registry_client,  
    avro_schema_str,  
    lambda obj, ctx: obj  
)

# Kafka Producer Configuration
producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,  
}

producer = SerializingProducer(producer_conf)

def generate_mock_txn():
    return {
        "txn_id": str(fake.uuid4()),  
        "timestamp": fake.iso8601(),
        "symbol": random.choice(["AAPL", "GOOGL", "AMZN", "TSLA", "MSFT"]),
        "price": round(random.uniform(100, 2000), 2),
        "volume": random.randint(10, 1000),
        "buyer": fake.name(),
        "seller": fake.name(),
    }

topic_name = "mock_txn_v1"

print(f"Sending mock transactions to Kafka topic: {topic_name}")

try:
    while True:
        txn_data = generate_mock_txn()
        producer.produce(
            topic_name,
            key=str(txn_data["txn_id"]), 
            value=txn_data,
            on_delivery=lambda err, msg: print(f"Delivery Error: {err}" if err else f"Sent: {msg.value()}"),
        )
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped Producer")
finally:
    producer.flush()
