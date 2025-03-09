from confluent_kafka import Producer
import json
import time

# ตั้งค่าการเชื่อมต่อ Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC = "mock_txn_v1"

producer_config = {
    "bootstrap.servers": KAFKA_BROKER
}

producer = Producer(producer_config)

# ข้อมูลจำลอง 20 ชุด (เรียงข้อมูลแบบไม่มี order_type ขึ้นก่อน)
test_data = [
    {"txn_id": "txn_011", "timestamp": "2025-03-10T13:00:00", "symbol": "AAPL", "price": 250.75, "volume": 120, "buyer": "Adam", "seller": "Brian"},
    {"txn_id": "txn_012", "timestamp": "2025-03-10T13:01:00", "symbol": "GOOGL", "price": 1800.50, "volume": 80, "buyer": "Cathy", "seller": "David"},
    {"txn_id": "txn_013", "timestamp": "2025-03-10T13:02:00", "symbol": "TSLA", "price": 750.30, "volume": 200, "buyer": "Ellen", "seller": "Frank"},
    {"txn_id": "txn_014", "timestamp": "2025-03-10T13:03:00", "symbol": "AMZN", "price": -20.00, "volume": 50, "buyer": "Grace", "seller": "Hank"},
    {"txn_id": "txn_015", "timestamp": "2025-03-10T13:04:00", "symbol": "MSFT", "price": 600.20, "volume": -10, "buyer": "Ivy", "seller": "Jack"},
    {"txn_id": "txn_016", "timestamp": "2025-03-10T13:05:00", "symbol": "FB", "price": "NaN", "volume": 90, "buyer": "Ken", "seller": "Leo"},
    {"txn_id": "txn_017", "timestamp": "2025-03-10T13:06:00", "symbol": "NFLX", "price": 399.99, "volume": "text instead of int", "buyer": "Mia", "seller": "Nick"},
    {"txn_id": "txn_018", "timestamp": "2025-03-10T13:07:00", "symbol": "GOOGL", "price": 1200.00, "volume": 0, "buyer": "Olivia", "seller": "Peter"},
    {"txn_id": "txn_019", "timestamp": "2025-03-10T13:08:00", "symbol": "TSLA", "price": 800.75, "volume": 150, "buyer": "Quinn", "seller": "Ryan12345"},
    {"txn_id": "txn_020", "timestamp": "2025-03-10T13:09:00", "symbol": "AAPL", "price": 1025.00, "volume": 175, "buyer": "Sophia", "seller": "Tom"},
    {"txn_id": "txn_001", "timestamp": "2025-03-10T12:34:56", "symbol": "AAPL", "price": 150.5, "volume": 100, "buyer": "Alice", "seller": "Bob", "order_type": "LIMIT"},
    {"txn_id": "txn_002", "timestamp": "2025-03-10T12:35:56", "symbol": "GOOGL", "price": 1200.75, "volume": 50, "buyer": "Charlie", "seller": "Dave", "order_type": "MARKET"},
    {"txn_id": "txn_003", "timestamp": "2025-03-10T12:36:56", "symbol": "TSLA", "price": -5.0, "volume": 20, "buyer": "Eve", "seller": "Frank", "order_type": "LIMIT"},
    {"txn_id": "txn_004", "timestamp": "2025-03-10T12:37:56", "symbol": "AMZN", "price": 500.25, "volume": -30, "buyer": "Grace", "seller": "Hank", "order_type": "LIMIT"},
    {"txn_id": "txn_005", "timestamp": "2025-03-10T12:38:56", "symbol": "MSFT", "price": 700.50, "volume": 0, "buyer": "Ivy", "seller": "Jack", "order_type": "MARKET"},
    {"txn_id": "txn_006", "timestamp": "2025-03-10T12:39:56", "symbol": "FB", "price": 250.10, "volume": 150, "buyer": "Ken", "seller": "Leo", "order_type": "LIMIT"},
    {"txn_id": "txn_007", "timestamp": "2025-03-10T12:40:56", "symbol": "NFLX", "price": "not_a_number", "volume": 80, "buyer": "Mia", "seller": "Nick", "order_type": "LIMIT"},
    {"txn_id": "txn_008", "timestamp": "2025-03-10T12:41:56", "symbol": "GOOGL", "price": 1300.55, "volume": "string_instead_of_int", "buyer": "Olivia", "seller": "Peter", "order_type": "MARKET"},
    {"txn_id": "txn_009", "timestamp": "2025-03-10T12:42:56", "symbol": "TSLA", "price": 999.99, "volume": 200, "buyer": "Quinn", "seller": "Ryan", "order_type": "LIMIT"},
    {"txn_id": "txn_010", "timestamp": "2025-03-10T12:43:56", "symbol": "AAPL", "price": 1500.75, "volume": 10, "buyer": "Sophia", "seller": "Tom", "order_type": "UNKNOWN_TYPE"}
]

# ส่งข้อมูลไป Kafka
for record in test_data:
    producer.produce(TOPIC, key=record["txn_id"], value=json.dumps(record))
    print(f"Sent: {record}")
    time.sleep(1)

producer.flush()
print("All test data sent to Kafka successfully!")
