from kafka import KafkaProducer
def publish_message(producer_instance, topic_name, key, value):
    try:
        producer_instance.send(topic_name,key=str.encode(key),value=str.encode(value))
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
