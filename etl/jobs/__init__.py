from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

from etl.dependencies.settings import APP_NAME, JAR_PACKAGES, SPARK_CONFIGS, \
    SPARK_FILES, SPARK_MASTER
from etl.dependencies.spark import start_spark


class BaseETL:
    def __init__(self, enable_hive: bool = False) -> None:
        self.start_session(enable_hive=enable_hive)

    def start_session(self, enable_hive: bool = False):
        self.spark, self.logger, self.etl_config = start_spark(
            app_name=APP_NAME, master=SPARK_MASTER, jar_packages=JAR_PACKAGES,
            files=SPARK_FILES, spark_config=SPARK_CONFIGS,
            enable_hive=enable_hive)

    def merge_into(self, source_table: DataFrame, new_table: DataFrame, p_key: str):
        '''Resolve SCD type 2
        '''
        if len(source_table.head(1)) == 0:
            return new_table

        # join outer source and new
        join_df = source_table.alias('src') \
            .join(new_table.alias('new'), col(f'src.{p_key}') == col(f'new.{p_key}'), 'outer') \
            .withColumn('action', when(col(f'src.{p_key}').isNull(), 'INSERT')
                        .when(col(f'src.{p_key}') == col(f'new.{p_key}'), 'UPDATE')
                        .otherwise('UNCHANGE'))

        # new records
        insert_df = join_df.filter(col('action') == 'INSERT') \
            .select('new.*')

        # update records
        update_df = join_df.filter(col('action') == 'UPDATE') \
            .select('new.*')

        # unchange records
        unchange_df = join_df.filter(col('action') == 'UNCHANGE') \
            .select('src.*')

        return reduce(DataFrame.union, [unchange_df, insert_df, update_df])

    def stop(self):
        return self.spark.stop()
