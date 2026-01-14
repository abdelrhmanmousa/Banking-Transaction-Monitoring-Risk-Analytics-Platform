import time
import os
import random
import uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# 1. Configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(BASE_DIR, "..", "schema", "transaction.avsc")

# 2. Load the schema from the file
with open(SCHEMA_PATH) as f:
    schema_str = f.read()


# 3. Create Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# 4. Producer Configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'value.serializer': avro_serializer
}

def generate_transaction():
    transaction_types = ['TRANSFER', 'WITHDRAWAL', 'PAYMENT', 'DEPOSIT']
    merchants = ['GROCERY', 'ELECTRONICS', 'LUXURY', 'GAMING', 'RESTAURANT']
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC-{random.randint(1000, 9999)}",
        "amount": float(random.uniform(10.0, 5000.0)),
        "currency": "USD",
        "transaction_type": random.choice(transaction_types),
        "merchant_category": random.choice(merchants),
        "location": random.choice(['New York', 'London', 'Paris', 'Tokyo', 'Unknown']),
        "timestamp": int(time.time() * 1000),
        "is_international": random.choice([True, False])
    }

def main():
    producer = SerializingProducer(producer_conf)
    topic = "banking_transactions"

    print(f"Starting Modern Bank Producer... Sending to {topic}")

    try:
        while True:
            tx = generate_transaction()
            
            # Risk Simulation: Large amounts
            if random.random() < 0.1:
                tx['amount'] = float(random.uniform(10000, 50000))

            producer.produce(topic=topic, value=tx)
            print(f"Produced: {tx['account_id']} | {tx['amount']:.2f} {tx['currency']}")
            
            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping Producer...")

if __name__ == "__main__":
    main()