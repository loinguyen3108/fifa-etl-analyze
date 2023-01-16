from functools import cached_property

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count, lit

from etl.dependencies.settings import DATALAKE_PATH, HDFS_MASTER
from etl.jobs import BaseETL
from etl.services.hdfs.hdfs import HDFSSerivce


class Ingestion(BaseETL):

    def __init__(self) -> None:
        super().__init__()

    @cached_property
    def hdfs_service(self):
        return HDFSSerivce()

    def extract(self, file_path: str):
        return self.spark.read.option('header', True).csv(file_path)

    def transform(self, df: DataFrame, fifa_version: int, gender: int):
        return df.withColumn('fifa_version', lit(fifa_version)) \
            .withColumn('gender', lit(gender))

    def load(self, df: DataFrame, table_name: str):
        row_exec = df.select(count('*')).collect()[0][0]
        file_path = f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}'
        df.write.option('header', True) \
            .partitionBy('fifa_version') \
            .mode('append') \
            .parquet(file_path)
        self.logger.info(
            f'Ingest data success in DataLake: {file_path} with {row_exec} row(s).')

    def run(self, file_path: str, table_name: str, fifa_version: int, gender: int):
        self.logger.info(f'Ingest data from (table={table_name}, fifa_version={fifa_version}), '
                         f'file={file_path})...')
        df = self.extract(file_path)
        if len(df.head(1)) == 0:
            return
        df = self.transform(df=df, fifa_version=fifa_version, gender=gender)
        self.load(df=df, table_name=table_name)
        self.stop()
