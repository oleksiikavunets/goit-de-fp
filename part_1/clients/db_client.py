from environs import env
from pyspark.sql import DataFrame, SparkSession

env.read_env()


class DbClient:
    def __init__(self):
        self._jdbc_url = env("jdbc_url")
        self._jdbc_user = env("jdbc_user")
        self._jdbc_password = env("jdbc_password")

    def write(self, table_name: str, df: DataFrame):
        df.write \
            .format("jdbc") \
            .option("url", self._jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", self._jdbc_user) \
            .option("password", self._jdbc_password) \
            .mode("append") \
            .save()

    def read(self, table_name: str) -> DataFrame:
        spark = SparkSession.builder \
            .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
            .appName("JDBCToKafka") \
            .getOrCreate()

        df = spark.read.format('jdbc').options(
            url=self._jdbc_url,
            driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
            dbtable=table_name,
            user=self._jdbc_user,
            password=self._jdbc_password) \
            .load()

        df.show()

        return df
