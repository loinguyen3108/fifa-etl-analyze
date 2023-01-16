from os import environ

# Hadoop Env Var
HDFS_URL = environ.get('HDFS_URL', 'http://localhost:9870')
HDFS_MASTER = environ.get('HDFS_MASTER', 'hdfs://localhost:9000')
DATALAKE_PATH = 'user/datalake'

# Spark Env Var
APP_NAME = 'fifa_app'
JAR_PACKAGES = []
SPARK_FILES = []
SPARK_CONFIGS = {
    'spark.sql.warehouse.dir': f'{HDFS_MASTER}/user/hive/warehouse'
}
SPARK_MASTER = 'spark://loinguyen:7077'
