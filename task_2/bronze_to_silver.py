from src.dataframe_io.fs import Fs
from src.tables.athlete_bio_table import AthletesBioTable
from src.tables.athlete_event_results_table import AthleteEventResultsTable

fs = Fs()

# 2. Написати файл bronze_to_silver.py. Він має:
# - зчитувати таблицю bronze,
# - виконувати функцію чистки тексту для всіх текстових колонок,
# - робити дедублікацію рядків,
# - записувати таблицю в папку silver/{table}, де {table} — ім’я таблиці.

AthletesBioTable(input_path='bronze/athletes_bio', output_path='silver/athletes_bio') \
    .read(fs) \
    .filter() \
    .clean_str_cols() \
    .drop_duplicates() \
    .write(fs) \
    .df.show()

AthleteEventResultsTable(input_path='bronze/athlete_event_results', output_path='silver/athlete_event_results') \
    .read(fs) \
    .clean_str_cols() \
    .drop_duplicates() \
    .write(fs) \
    .df.show()
