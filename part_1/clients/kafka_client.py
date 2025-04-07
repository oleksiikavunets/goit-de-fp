import uuid

from environs import env
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit, struct, to_json
from pyspark.sql.types import StructType

from part_1.clients.spark_builder import spark_session

env.read_env()


# os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,'
#                                      'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell')


class KafkaClient:
    def __init__(self):
        self._config = {
            'kafka.bootstrap.servers': env.list('BOOTSTRAP_SERVERS')[0],
            'kafka.security.protocol': env('KAFKA_SECURITY_PROTOCOL'),
            'kafka.sasl.mechanism': env('SASL_MECHANISM'),
            'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                      f'username="{env("KAFKA_USER")}" password="{env("KAFKA_PASS")}";'
        }

    def write(self, topic: str, df: DataFrame):
        prepared_df = (
            df.select(to_json(struct([c for c in df.columns])).alias("value"))
            .withColumn("key", lit(str(uuid.uuid4())))
        )

        (
            prepared_df.write
            .format("kafka")
            .options(topic=topic, **self._config)
            .option("checkpointLocation", "/tmp/checkpoint-2")
            .save()
        )

    def read(self, topic, schema: StructType = None) -> DataFrame:
        df = (
            spark_session()
            .readStream
            .format("kafka")
            .options(subscribe=topic, **self._config)
            .load()
        )

        if schema:
            df = df.select(
                from_json(col("value").cast("string"), schema).alias("data"), col("timestamp")
            ).select("data.*")

        return df
