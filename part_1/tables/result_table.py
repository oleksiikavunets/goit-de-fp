from pyspark.sql.functions import avg, current_timestamp

from part_1.tables.table import Table


class ResultTable(Table):
    name = 'oleksii_k_agg_athlete_event_results'

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
