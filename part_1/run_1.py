from part_1.clients.db_client import DbClient

from part_1.clients.kafka_client import KafkaClient
from part_1.tables.athlete_bio_table import AthletesBioTable
from part_1.tables.athlete_event_results_table import AthleteEventResultsTable
from part_1.tables.result_table import ResultTable

db_client = DbClient()
kafka_client = KafkaClient()

athletes_bio = AthletesBioTable().read(db_client)

AthleteEventResultsTable() \
    .read(db_client) \
    .write(kafka_client)

kfk_athlete_event_results = AthleteEventResultsTable().read(kafka_client)

joined = athletes_bio.filter().join(kfk_athlete_event_results, on='athlete_id')

ResultTable(joined) \
    .aggregate() \
    .write_stream(kafka_client, db_client)

print()
