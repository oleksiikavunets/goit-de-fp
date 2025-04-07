from environs import env
from kafka.admin import KafkaAdminClient, NewTopic

env.read_env()


class KafkaAdmin:

    def __init__(self):
        self._client = KafkaAdminClient(
            bootstrap_servers=env.list('BOOTSTRAP_SERVERS'),
            security_protocol=env('KAFKA_SECURITY_PROTOCOL'),
            sasl_mechanism=env('SASL_MECHANISM'),
            sasl_plain_username=env('KAFKA_USER'),
            sasl_plain_password=env('KAFKA_PASS')
        )

    def create_topics(self, *topics_names, num_partitions=2, replication_factor=1):
        topics = [
            NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            for topic_name in topics_names
        ]

        try:
            self._client.create_topics(new_topics=topics, validate_only=False)
            print(f"Created new topics successfully successfully.")
        except Exception as e:
            print(f"An error occurred: {e}\n\n\n")

        return self

    def close(self):
        self._client.close()
