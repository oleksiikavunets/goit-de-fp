from src.dataframe_io.fs import Fs
from src.tables.athlete_bio_table import AthletesBioTable
from src.tables.athlete_event_results_table import AthleteEventResultsTable
from src.tables.result_table import AggResultTable

fs = Fs()

# 3. Написати файл silver_to_gold.py. Він має:
# - зчитувати дві таблиці: silver/athlete_bio та silver/athlete_event_results,
# - робити join за колонкою athlete_id,
# - для кожної комбінації цих 4 стовпчиків — sport, medal, sex, country_noc — знаходити середні значення weight і height,
# - додати колонку timestamp з часовою міткою виконання програми,
# - записувати дані в gold/avg_stats.

athletes_bio = AthletesBioTable(input_path='silver/athletes_bio').read(fs)

athlete_event_results = AthleteEventResultsTable(input_path='silver/athlete_event_results').read(fs)

joined = athletes_bio.join(athlete_event_results, on='athlete_id')

AggResultTable(joined, output_path='gold/avg_stats') \
    .aggregate() \
    .write(fs) \
    .df.show()
