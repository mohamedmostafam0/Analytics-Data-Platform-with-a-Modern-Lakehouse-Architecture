import os
import time
import random
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

def signal_handler(sig, frame):
    print('Shutting down gracefully...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Define the Avro schema matching Flink's expectations
LOGIN_EVENT_SCHEMA_STR = """
{
  "namespace": "com.shop.events",
  "name": "LoginEvent",
  "type": "record",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "ip", "type": "string"},
    {"name": "device", "type": "string"},
    {"name": "platform", "type": "string"},
    {"name": "user_agent", "type": "string"}
  ]
}
"""

def main():
    bootstrap_servers = os.environ.get('KAFKA_BROKERS', 'localhost:9092')
    schema_registry_url = os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    TOPIC = 'login-events'

    print(f"Connecting to Kafka at {bootstrap_servers} and Schema Registry at {schema_registry_url}...")
    
    # Wait for Schema Registry to be available via explicit HTTP check
    import requests
    while True:
        try:
            response = requests.get(schema_registry_url)
            if response.status_code == 200:
                print("Schema Registry is up!")
                break
        except requests.exceptions.ConnectionError:
            print(f"Waiting for Schema Registry at {schema_registry_url}... retrying in 5 seconds.")
            time.sleep(5)

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    
    avro_serializer = AvroSerializer(
        schema_registry_client,
        LOGIN_EVENT_SCHEMA_STR,
        lambda event, ctx: event
    )
    
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    devices = ['iPhone 15', 'Samsung Galaxy', 'MacBook Pro', 'Windows Laptop']
    ips = ['192.168.1.100', '203.0.113.22', '10.0.0.99', '198.51.100.14', '8.8.8.8']
    platforms = ['ios', 'android', 'web']
    
    print(f"Starting to send Avro events to topic: {TOPIC}")
    
    try:
        while True:
            # Flink `TIMESTAMP(3)` expects format like '2024-02-20T12:00:00.123Z' or similar ISO-8601
            # String representation is safe for Flink SQL timestamp parsing
            ts = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            
            event = {
                "user_id": f"user_{random.randint(1,5)}",
                "timestamp": ts,
                "ip": random.choice(ips),
                "device": random.choice(devices),
                "platform": random.choice(platforms),
                "user_agent": "Mozilla/5.0"
            }
            producer.produce(topic=TOPIC, key=event["user_id"], value=event)
            # Confluent kafka's produce is completely asynchronous
            producer.poll(0) 
            print("Sent Avro:", event)
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print("Producer closed.")

if __name__ == '__main__':
    main()
