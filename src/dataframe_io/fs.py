from typing import NoReturn

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from src.dataframe_io.dataframe_io import DataFrameIO
from src.dataframe_io.spark_builder import spark_session


class Fs(DataFrameIO):
    def read(self, from_: str, schema: StructType = None) -> DataFrame:
        df = spark_session().read.parquet(f'{from_}.parquet')

        df = self._apply_schema(df, schema)

        return df

    def write(self, to_: str, df: DataFrame) -> NoReturn:
        df.write.mode('overwrite').parquet(f'{to_}.parquet')
