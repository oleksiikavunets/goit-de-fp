from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class Table:
    schema: StructType
    name: str
    df: DataFrame

    def __init__(self, df=None):
        self.df = df

    def read(self, client):
        self.df = client.read(self.name, self.schema)

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
        joined_df = self.df.join(other.df, on=on, how=how)

        dupl_cols = set(self.df.columns) & set(other.df.columns)

        for col in dupl_cols:
            joined_df = joined_df.drop(other.df[col])

        return joined_df
