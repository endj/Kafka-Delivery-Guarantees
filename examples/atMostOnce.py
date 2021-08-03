from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

partition = TopicPartition('test', 0)
servers = ['localhost:9091', 'localhost:9092', 'localhost:9093']
consumer = KafkaConsumer(bootstrap_servers=servers,
                         enable_auto_commit=False,
                         group_id='testgroup',
                         auto_offset_reset='earliest')
consumer.assign([partition])

result = consumer.poll(timeout_ms=3000,
                       max_records=1,
                       update_offsets=False)

if result and  result[partition]:
    record = result[partition][0]
    next_offset = record.offset + 1
    consumer.commit({
                partition: OffsetAndMetadata(next_offset, None)
                })
    # process(record)
    print("Processed record ", record)
else:
	print("No new records on partition", partition)


