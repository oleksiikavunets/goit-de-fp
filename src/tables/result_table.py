from pyspark.sql.functions import avg, current_timestamp

from src.tables.table import Table


class AggResultTable(Table):
    _default_io = 'oleksii_k_agg_athlete_event_results'

    def aggregate(self):
        self.df = (
            self.df.groupBy('sport', 'medal', 'sex', 'country_noc')
            .agg(
                avg('height').alias('avg_height'),
                avg('weight').alias('avg_weight')
            )
            .withColumn('timestamp', current_timestamp())
        )

        return self
