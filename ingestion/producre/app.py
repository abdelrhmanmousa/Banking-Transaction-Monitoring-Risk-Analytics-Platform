import time, random, uuid , os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# 1) Schema Registry
sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

def get_serializer(schema_file):
    with open("../schema/" + schema_file, "r") as f:
        return AvroSerializer(sr_client, f.read())

tx_serializer = get_serializer("transaction.avsc")
login_serializer = get_serializer("user_login.avsc")
activity_serializer = get_serializer("account_activity.avsc")

# 2) Producer base config (2 brokers for HA)
base_conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093',

    # Reliability + ordering-safe retries
    'acks': 'all',
    'enable.idempotence': True,

    # Batching (good throughput, still low latency)
    'linger.ms': 10,
    'compression.type': 'snappy',
}

def main():
    tx_prod    = SerializingProducer({**base_conf, 'value.serializer': tx_serializer})
    login_prod = SerializingProducer({**base_conf, 'value.serializer': login_serializer})
    act_prod   = SerializingProducer({**base_conf, 'value.serializer': activity_serializer})

    accounts = [f"ACC-{i}" for i in range(1000, 2000)]
    locations = ["New York", "London", "Paris", "Tokyo", "Berlin", "Unknown"]

    last_flush = time.time()

    print("Real-time Multi-stream Ingestion Started...")

    try:
        while True:
            user = random.choice(accounts)
            now = int(time.time() * 1000)

            # 1) Login (30%)
            if random.random() < 0.3:
                login_data = {
                    "account_id": user,
                    "login_id": str(uuid.uuid4()),
                    "ip_address": f"192.168.1.{random.randint(1,255)}",
                    "location": random.choice(locations),
                    "device_type": random.choice(["Mobile", "Web"]),
                    "timestamp": now
                }
                login_prod.produce(topic="user_logins", key=user, value=login_data)

            # 2) Activity (10%)
            if random.random() < 0.1:
                act_data = {
                    "account_id": user,
                    "activity_type": "LIMIT_CHANGE",
                    "old_value": "5000",
                    "new_value": "20000",
                    "timestamp": now
                }
                act_prod.produce(topic="account_activity", key=user, value=act_data)

            # 3) Transaction (Always)
            tx_data = {
                "transaction_id": str(uuid.uuid4()),
                "account_id": user,
                "amount": float(random.uniform(10, 15000)),
                "currency": "USD",
                "transaction_type": "TRANSFER",
                "merchant_category": "RETAIL",
                "location": random.choice(locations),
                "timestamp": now,
                "is_international": random.choice([True, False])
            }
            tx_prod.produce(topic="banking_transactions", key=user, value=tx_data)

            # Serve delivery callbacks / internal queue
            tx_prod.poll(0)
            login_prod.poll(0)
            act_prod.poll(0)

            # Flush every 2 seconds (not every message)
            if time.time() - last_flush >= 2:
                tx_prod.flush(0)
                login_prod.flush(0)
                act_prod.flush(0)
                last_flush = time.time()

            time.sleep(0.5)

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush to ensure everything is delivered
        tx_prod.flush()
        login_prod.flush()
        act_prod.flush()
        print("Stopped Ingestion.")

if __name__ == "__main__":
    main()
