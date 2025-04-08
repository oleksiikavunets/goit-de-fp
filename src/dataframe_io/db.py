from environs import env
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from src.dataframe_io.dataframe_io import DataFrameIO
from src.dataframe_io.spark_builder import spark_session



class Db(DataFrameIO):
    def __init__(self):
        self._config = {
            'driver': 'com.mysql.cj.jdbc.Driver',
            'url': env("JDBC_URL"),
            'user': env("JDBC_USER"),
            'password': env("JDBC_PASS"),
        }

    def write(self, to_: str, df: DataFrame):
        (df.write
         .format("jdbc")
         .options(dbtable=to_, **self._config)
         .mode("append")
         .save())

    def read(self, from_: str, schema: StructType = None) -> DataFrame:
        df = (spark_session().read
              .format('jdbc')
              .options(dbtable=from_, **self._config)
              .load())

        df = self._apply_schema(df, schema)

        df.show()

        return df
