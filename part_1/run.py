from part_1.clients.db_client import DbClient
from part_1.clients.kafka_admin_client import KafkaAdmin

from part_1.clients.kafka_client import KafkaClient
from part_1.clients.kafka_consumer_client import KafkaConsumerClient
from part_1.tables.athlete_bio_table import AthletesBioTable
from part_1.tables.athlete_event_results_table import AthleteEventResultsTable
from part_1.tables.result_table import ResultTable

KafkaAdmin().create_topics('oleksii_k_agg_athlete_event_results').close()

db_client = DbClient()
kafka_client = KafkaClient()

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio
# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.
athletes_bio = AthletesBioTable().read(db_client).filter()

# 3. Зчитати дані з mysql таблиці athlete_event_results і записати в Kafka-топік athlete_event_results.
AthleteEventResultsTable() \
    .read(db_client) \
    .write(kafka_client)

# 3.1. Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results.
kfk_athlete_event_results = AthleteEventResultsTable().read(kafka_client)

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
joined = athletes_bio.join(kfk_athlete_event_results, on='athlete_id')

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.
# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
#    а) вихідний Kafka-топік,
#    b) базу даних.
ResultTable(joined) \
    .aggregate() \
    .write_stream(db_client, kafka_client)

KafkaConsumerClient().read('oleksii_k_agg_athlete_event_results')
