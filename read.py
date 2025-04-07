from pyspark.sql import SparkSession
from environs import env

env.read_env()
# # Налаштування конфігурації SQL бази даних
jdbc_url = env("jdbc_url")
jdbc_table = env("jdbc_table")
jdbc_user = env("jdbc_user")
jdbc_password = env("jdbc_password")

# # Створення Spark сесії



# Функція для обробки кожної партії даних
def foreach_batch_function(batch_df, batch_id):

    # Відправка збагачених даних до Kafka
    batch_df \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", topic) \
        .save()

    # Збереження збагачених даних до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{musql_server}:3306/{db}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

# Налаштування потоку даних для обробки кожної партії за допомогою вказаної функції
event_stream_enriched \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
