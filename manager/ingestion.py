from argparse import ArgumentParser
from datetime import datetime

from etl.jobs.ingestion.ingestion import Ingestion


arg_parser = ArgumentParser(description='Data Ingestion')
arg_parser.add_argument(
    '--file', dest='file',
    type=str, help='File path of raw data need ingest. Ex: file:/home/loinguyen/fifa15.csv')
arg_parser.add_argument(
    '--fifa-version', dest='fifa_version',
    type=str, help='Fifa version.')
arg_parser.add_argument(
    '--gender', dest='gender',
    type=int, help='Player gender.')
arg_parser.add_argument(
    '--table-name', dest='table_name',
    type=str, help='Table need ingest.')
args = arg_parser.parse_args()
_file = args.file
fifa_version = args.fifa_version
gender = args.gender
table_name = args.table_name

ingestion = Ingestion()
ingestion.run(file_path=_file, table_name=table_name,
              fifa_version=fifa_version, gender=gender)
