import json

from environs import env
from kafka import KafkaConsumer

env.read_env()

class KafkaConsumerClient:
    def __init__(self):
        self._consumer = KafkaConsumer(
            bootstrap_servers=env.list('BOOTSTRAP_SERVERS'),
            security_protocol=env('KAFKA_SECURITY_PROTOCOL'),
            sasl_mechanism=env('SASL_MECHANISM'),
            sasl_plain_username=env('KAFKA_USER'),
            sasl_plain_password=env('KAFKA_PASS'),
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda v: v.decode('utf-8') if v else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='oleksiik_fp_group'
        )

    def read(self, *topics):
        self._consumer.subscribe(list(topics))

        print(f"Subscribed to topics {topics}")

        try:
            for message in self._consumer:
                print(f"Received alert:\n{message.value}\n")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self._consumer.close()