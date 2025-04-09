import uuid

from environs import env
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, struct, to_json
from pyspark.sql.types import StructType

from src.dataframe_io.dataframe_io import DataFrameIO
from src.dataframe_io.dataframe_streaming_io import DataFrameStreamingIO
from src.dataframe_io.spark_builder import spark_session


class Kafka(DataFrameStreamingIO, DataFrameIO):
    def __init__(self):
        self._config = {
            'kafka.bootstrap.servers': env.list('BOOTSTRAP_SERVERS')[0],
            'kafka.security.protocol': env('KAFKA_SECURITY_PROTOCOL'),
            'kafka.sasl.mechanism': env('SASL_MECHANISM'),
            'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                      f'username="{env("KAFKA_USER")}" password="{env("KAFKA_PASS")}";'
        }

    def write(self, to_: str, df: DataFrame):
        prepared_df = (
            df.select(to_json(struct([c for c in df.columns])).alias("value"))
            .withColumn("key", lit(str(uuid.uuid4())))
        )

        (
            prepared_df.write
            .format("kafka")
            .options(topic=to_, checkpointLocation='/tmp/checkpoint-kfk-w', **self._config)
            .save()
        )

    def write_stream(self, to_: str, df: DataFrame):
        prepared_df = (
            df.select(to_json(struct([c for c in df.columns])).alias("value"))
            .withColumn("key", lit(str(uuid.uuid4())))
        )

        (
            prepared_df.writeStream
            .format("kafka")
            .options(topic=to_, checkpointLocation='/tmp/checkpoint-kfk-ws', **self._config)
            .outputMode("complete")
            .start()
            .awaitTermination()
        )

    def read(self, from_: str, schema: StructType = None) -> DataFrame:
        df = (
            spark_session()
            .read
            .format("kafka")
            .options(
                subscribe=from_,
                startingOffsets='latest',
                checkpointLocation='/tmp/checkpoint-kfk-r',
                **self._config
            )
            .load()
        )

        df = self._apply_schema(df, schema)

        return df

    def read_stream(self, from_: str, schema: StructType = None) -> DataFrame:
        df = (
            spark_session()
            .readStream
            .format("kafka")
            .options(
                subscribe=from_,
                startingOffsets='latest',
                checkpointLocation='/tmp/checkpoint-kfk-rs',
                **self._config
            )
            .load()
        )

        df = self._apply_schema(df, schema)

        return df
