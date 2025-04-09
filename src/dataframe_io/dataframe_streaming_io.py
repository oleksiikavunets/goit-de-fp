from abc import ABC, abstractmethod
from typing import NoReturn
from environs import env

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

env.read_env()


class DataFrameStreamingIO(ABC):
    @abstractmethod
    def read_stream(self, from_: str, schema: StructType = None) -> DataFrame:
        ...

    @abstractmethod
    def write_stream(self, to_: str, df: DataFrame) -> NoReturn:
        ...

    @staticmethod
    def _apply_schema(df: DataFrame, schema: StructType):
        if schema:
            df = df.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp")
            ).select("data.*")
        return df
