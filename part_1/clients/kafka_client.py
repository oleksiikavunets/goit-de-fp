import os
import uuid

from environs import env
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, struct, to_json

env.read_env()

os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,'
                                     'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell')


class KafkaClient:
    def __init__(self):
        self._bootstrap_servers = env.list('bootstrap_servers')
        self._kafka_security_protocol = env('kafka_security_protocol')
        self._sasl_mechanism = env('sasl_mechanism')
        self._user = env("kafka_user")
        self._pass = env("kafka_pass")

    def write(self, topic: str, df: DataFrame):
        prepared_df = (
            df.select(to_json(struct([c for c in df.columns])).alias("value"))
            .withColumn("key", lit(str(uuid.uuid4())))
        )

        (
            prepared_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", self._bootstrap_servers[0])
            .option("topic", topic)
            .option("kafka.security.protocol", self._kafka_security_protocol)
            .option("kafka.sasl.mechanism", self._sasl_mechanism)
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self._user}" password="{self._pass}";')
            .option("checkpointLocation", "/tmp/checkpoint-1")
            .save()
        )

    def read(self, topic) -> DataFrame:
        spark = (SparkSession.builder
                 .appName("KafkaStreaming")
                 .master("local[*]")
                 .getOrCreate())

        df = (
            spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", self._bootstrap_servers[0])
            .option("kafka.security.protocol", self._kafka_security_protocol)
            .option("kafka.sasl.mechanism", self._sasl_mechanism)
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self._user}" password="{self._pass}";')
            .option("subscribe", topic)
            .load()
        )

        df.show()

        return df
