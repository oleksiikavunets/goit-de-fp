from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, BooleanType

from part_1.tables.table import Table


class AthleteEventResultsTable(Table):
    name = 'athlete_event_results'
    schema = StructType(
        [
            StructField('edition', StringType(), True),
            StructField('edition_id', IntegerType(), True),
            StructField('country_noc', StringType(), True),
            StructField('sport', StringType(), True),
            StructField('event', StringType(), True),
            StructField('result_id', LongType(), True),
            StructField('athlete', StringType(), True),
            StructField('athlete_id', IntegerType(), True),
            StructField('pos', StringType(), True),
            StructField('medal', StringType(), True),
            StructField('isTeamSport', StringType(), True)
        ]
    )
