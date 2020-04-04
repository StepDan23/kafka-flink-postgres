from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads


def connect_kafka_producer(servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers,
                                  value_serializer=lambda v: dumps(v).encode('utf-8')
                                  )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def connect_kafka_consumer(topic, servers):
    _consumer = None
    try:
        _consumer = KafkaConsumer(topic, auto_offset_reset='earliest',
                                  value_deserializer=lambda m: loads(m.decode('utf-8')),
                                  bootstrap_servers=servers,
                                  consumer_timeout_ms=600
                                  )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


def publish_message(producer_instance, topic_name, key, value):
    try:
        producer_instance.send(topic_name, {key: value})
        producer_instance.flush()
        print(f"Message '{key}: {value}' published successfully.")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def read_messages(consumer_instance):
    messages = None
    try:
        messages = [mess.value for mess in consumer_instance]
    except Exception as ex:
        print('Exception in reading message')
        print(str(ex))
    return messages
