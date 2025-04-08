from typing import NoReturn

import requests
from environs import env
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from src.dataframe_io.dataframe_io import DataFrameIO
from src.dataframe_io.spark_builder import spark_session



class Ftp(DataFrameIO):

    def __init__(self):
        self._url = env('FTP_SERVER_URL')

    def read(self, from_: str, schema: StructType = None) -> DataFrame:
        self._download_data(from_)

        df = spark_session().read.option("header", True).csv(f'{from_}.csv')

        df = self._apply_schema(df, schema)

        return df

    def write(self, to_: str, df: DataFrame) -> NoReturn:
        raise NotImplemented()

    def _download_data(self, _path):
        f_name = f'{_path}.csv'
        downloading_url = f'{self._url}{f_name}'

        print(f"Downloading from {downloading_url}")
        response = requests.get(downloading_url)

        if response.status_code == 200:
            with open(f_name, 'wb') as file:
                file.write(response.content)
            print(f"File downloaded successfully and saved as {_path}")
        else:
            exit(f"Failed to download the file. Status code: {response.status_code}")
