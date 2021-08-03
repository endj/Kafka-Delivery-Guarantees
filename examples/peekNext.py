from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

partition = TopicPartition('test', 0)
bootstrap_servers = ['localhost:9091', 'localhost:9092', 'localhost:9093']
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         enable_auto_commit=False, group_id='testgroup', auto_offset_reset='earliest')
consumer.assign([partition])

result = consumer.poll(timeout_ms=3000, max_records=1, update_offsets=False)

if result:
    record = result[partition][0]
    print(record)
else:
    print("No new records since last offset", consumer.committed(partition))
