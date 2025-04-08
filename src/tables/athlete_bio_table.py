from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType

from src.tables.table import Table


class AthletesBioTable(Table):
    _default_io = 'athlete_bio'
    schema = StructType(
        [
            StructField('athlete_id', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('sex', StringType(), True),
            StructField('born', StringType(), True),
            StructField('height', DoubleType(), True),
            StructField('weight', DoubleType(), True),
            StructField('country', StringType(), True),
            StructField('country_noc', StringType(), True),
            StructField('description', StringType(), True),
            StructField('special_notes', StringType(), True)
        ]
    )

    def filter(self):
        self.df = self.df.filter(
            col("height").isNotNull() &
            col("weight").isNotNull()
        )

        return self
