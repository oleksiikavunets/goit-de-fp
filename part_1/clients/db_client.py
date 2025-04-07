from environs import env
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from part_1.clients.spark_builder import spark_session

env.read_env()


class DbClient:
    def __init__(self):
        self._config = {
            'driver': 'com.mysql.cj.jdbc.Driver',
            'url': env("JDBC_URL"),
            'user': env("JDBC_USER"),
            'password': env("JDBC_PASS"),
        }

    def write(self, table_name: str, df: DataFrame):
        (df.write
         .format("jdbc")
         .options(dbtable=table_name, **self._config)
         .mode("append")
         .save())

    def read(self, table_name: str, schema: StructType = None) -> DataFrame:
        df = (spark_session().read
              .format('jdbc')
              .options(dbtable=table_name, **self._config)
              .load())

        if schema:
            for field in schema.fields:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        df.show()

        return df
