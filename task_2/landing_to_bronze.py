from src.dataframe_io.fs import Fs
from src.dataframe_io.ftp import Ftp
from src.tables.athlete_bio_table import AthletesBioTable
from src.tables.athlete_event_results_table import AthleteEventResultsTable

ftp = Ftp()
fs = Fs()

# 1. Написати файл landing_to_bronze.py. Він має:
# - завантажувати файл з ftp-сервера в оригінальному форматі csv,
# - за допомогою Spark прочитати csv-файл і зберегти його у форматі parquet у папку bronze/{table}, де {table} — ім’я таблиці.

AthletesBioTable(output_path='bronze/athletes_bio') \
    .read(ftp) \
    .write(fs) \
    .df.show()

AthleteEventResultsTable(output_path='bronze/athlete_event_results') \
    .read(ftp) \
    .write(fs) \
    .df.show()
