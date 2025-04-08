from pyspark.sql import SparkSession

SPARK_JARS = ['com.mysql:mysql-connector-j:8.0.32',
              'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1',
              'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1']


def spark_session():
    return (SparkSession.builder
            .config('spark.jars.packages', ','.join(SPARK_JARS))
            .appName('FpApp')
            .getOrCreate())
