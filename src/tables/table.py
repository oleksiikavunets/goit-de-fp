import re
from typing import Tuple
from typing import Self

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType

from src.dataframe_io.dataframe_io import DataFrameIO


class Table:
    _schema: StructType
    _default_io: str

    def __init__(self, df: DataFrame = None, input_path: str = None, output_path: str = None):
        self.df = df
        self._input = input_path or self._default_io
        self._output = output_path or self._default_io

    @property
    def schema(self):
        if not self._schema and self.df:
            self._schema = self.df.schema
        return self._schema

    def read(self, reader: DataFrameIO) -> Self:
        self.df = reader.read(self._input, self.schema)

        return self

    def write(self, *writers: *Tuple[DataFrameIO]) -> Self:
        [writer.write(self._output, self.df) for writer in writers]
        return self

    def write_stream(self, *writers: *Tuple[DataFrameIO]) -> Self:
        def foreach_batch_function(batch_df, batch_id):
            [
                writer.write(self._output, batch_df) for writer in writers
            ]

        (self.df
         .writeStream
         .foreachBatch(foreach_batch_function)
         .option('checkpointLocation', '/tmp/checkpoint-tbl-w')
         .outputMode("complete")
         .start()
         .awaitTermination())

        return self

    def filter(self) -> Self:
        ...
        return self

    def join(self, other, on, how='inner') -> DataFrame:
        joined_df = self.df.join(other.df, on=on, how=how)

        dupl_cols = set(self.df.columns) & set(other.df.columns)

        for col in dupl_cols:
            joined_df = joined_df.drop(other.df[col])

        return joined_df

    def clean_str_cols(self):
        def _clean(text):
            return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

        clean_udf = udf(_clean, StringType())
        string_cols = [f.name for f in self.df.schema.fields if isinstance(f.dataType, StringType)]

        for c in string_cols:
            self.df = self.df.withColumn(c, clean_udf(col(c)))

        return self

    def drop_duplicates(self):
        self.df = self.df.dropDuplicates()
        return self
