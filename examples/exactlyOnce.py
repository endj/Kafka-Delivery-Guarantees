from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import asyncio
import asyncpg
import datetime
import json

async def main():
    conn = await asyncpg.connect(user='postgres', password='123',
                                 database='postgres', host='localhost')

    partition = TopicPartition('test', 0)
    servers = ['localhost:9091', 'localhost:9092', 'localhost:9093']
    consumer = KafkaConsumer(bootstrap_servers=servers,
                         enable_auto_commit=False,
                         group_id='testgroup',
                         auto_offset_reset='earliest')
    consumer.assign([partition])

    print("Waiting for messages..")
    for msg in consumer:
        body = msg.value.decode('utf-8')
        payload = json.loads(body)
        print("persisting",payload)
        await conn.execute('''INSERT INTO messages(idempotency_key, message)
                          SELECT $1, $2 WHERE NOT EXISTS
                          ( SELECT 1 FROM messages WHERE idempotency_key = $1)''',
                       payload['idempotency_key'],
                       payload['message'])


asyncio.get_event_loop().run_until_complete(main())
