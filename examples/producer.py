from kafka import KafkaProducer
import uuid
import json

bootstrap_servers = ['localhost:9091', 'localhost:9092', 'localhost:9093']
topic_name = "test"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks='all')

message = input("Enter message to send: ")
payload = {
    "idempotency_key": str(uuid.uuid4()),
    "message": message
}

p = json.dumps(payload)
producer.send(topic_name, p.encode(), partition=0)
producer.flush()
print(f'Sent {message} to topic {topic_name}')
