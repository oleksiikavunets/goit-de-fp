from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json


class Table:
    schema: str
    name: str
    df: DataFrame

    def __init__(self, df=None):
        self.df = df

    def read(self, client):
        self.df = client.read(self.name)

        if "value" in self.df.columns:
            self.df = self.df.select(
                from_json(col("value").cast("string"), self.schema).alias("data"), col("timestamp")
            ).select("data.*")

            self.df.show()

        return self

    def write(self, *clients):
        [client.write(self.name, self.df) for client in clients]
        return self

    def write_stream(self, *clients):
        def foreach_batch_function(batch_df, batch_id):
            [
                client.write(self.name, batch_df) for client in clients
            ]

        (self.df
         .writeStream
         .foreachBatch(foreach_batch_function)
         .outputMode("update")
         .start()
         .awaitTermination())

        return self

    def filter(self):
        ...
        return self

    def join(self, other, on, how='inner'):
        d1 = set(self.df.toPandas()['athlete_id'])
        d2 = set(other.df.toPandas()['athlete_id'])

        print(d1 & d2)
        joined_df = self.df.join(other.df, on=on, how=how)

        joined_df.show()

        seen = set()
        unique_cols = []

        [
            (unique_cols.append(col_name),
             seen.add(col_name))
            for col_name in joined_df.columns if col_name not in seen
        ]

        joined_df = joined_df.select(*unique_cols)

        joined_df.show()

        return joined_df
