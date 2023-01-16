from argparse import ArgumentParser
from datetime import datetime

from etl.jobs.transform.transform import TransformETL


arg_parser = ArgumentParser(description='Data ETL')
arg_parser.add_argument('--exec-date', dest='exec_date', default=str(datetime.utcnow().date()),
                        type=str, help='Format: YYYY:MM:DD')
arg_parser.add_argument(
    '--fifa-version', dest='fifa_version',
    type=int, help='Fifa version.')
arg_parser.add_argument(
    '--init', dest='init',
    type=bool, default=False, help='Init dim_date.'
)
args = arg_parser.parse_args()
exec_date = datetime.fromisoformat(args.exec_date)
fifa_version = args.fifa_version
init = args.init
etl = TransformETL(exec_date=exec_date, year_version=fifa_version)
if init:
    etl.run_init()
else:
    etl.run()
