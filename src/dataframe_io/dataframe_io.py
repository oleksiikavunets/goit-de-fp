from abc import ABC, abstractmethod
from typing import NoReturn
from environs import env

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

env.read_env()


class DataFrameIO(ABC):
    @abstractmethod
    def read(self, from_: str, schema: StructType = None) -> DataFrame:
        ...

    @abstractmethod
    def write(self, to_: str, df: DataFrame) -> NoReturn:
        ...

    @staticmethod
    def _apply_schema(df: DataFrame, schema: StructType):
        if schema:
            for field in schema.fields:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df
