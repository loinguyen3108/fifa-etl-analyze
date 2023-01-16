import re
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, count, dayofmonth, month, when, \
    year, split, upper, regexp_replace, udf
from pyspark.sql.types import BooleanType, IntegerType

from etl.dependencies.settings import DATALAKE_PATH, HDFS_MASTER
from etl.jobs import BaseETL

PLAYER = 'dim_player'
DATE = 'dim_date'
CLUB = 'dim_club'
NATIONALITY = 'dim_nationality'
INFORMATION = 'information'

POSITIONS = []


class TransformETL(BaseETL):
    def __init__(self, exec_date: datetime = None, year_version: int = None) -> None:
        super().__init__(enable_hive=True)
        self.exec_date = exec_date or datetime.utcnow().date()
        self.year_version = year_version

    def extract(self):
        self.fifa_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/fifa_dataset') \
            .filter(col('fifa_version') == self.year_version)
        self.player_df = self.fifa_df \
            .select('sofifa_id', 'short_name', 'long_name', 'dob', 'gender')
        self.club_df = self.fifa_df \
            .select('club_team_id', 'club_name', 'league_name')
        self.nationality_df = self.fifa_df \
            .select('nationality_id', 'nationality_name')
        self.infomation_df = self.fifa_df \
            .select('sofifa_id', 'player_positions', 'overall', 'potential', 'value_eur', 'wage_eur', 'age', 'height_cm', 'weight_kg', 'club_team_id',
                    'league_level', 'club_position', 'club_jersey_number', 'club_joined', 'club_contract_valid_until', 'nationality_id', 'nation_team_id',
                    'nation_position', 'nation_jersey_number', 'preferred_foot', 'weak_foot', 'skill_moves', 'international_reputation',
                    'work_rate', 'body_type', 'release_clause_eur', 'player_tags', 'player_traits', 'pace', 'shooting', 'passing', 'dribbling',
                    'defending', 'physic', 'attacking_crossing', 'attacking_finishing', 'attacking_heading_accuracy', 'attacking_short_passing',
                    'attacking_volleys', 'skill_dribbling', 'skill_curve', 'skill_fk_accuracy', 'skill_long_passing', 'skill_ball_control',
                    'movement_acceleration', 'movement_sprint_speed', 'movement_agility', 'movement_reactions', 'movement_balance', 'power_shot_power',
                    'power_jumping', 'power_stamina', 'power_strength', 'power_long_shots', 'mentality_aggression', 'mentality_interceptions',
                    'mentality_positioning', 'mentality_vision', 'mentality_penalties', 'mentality_composure', 'defending_marking_awareness',
                    'defending_standing_tackle', 'defending_sliding_tackle', 'goalkeeping_diving', 'goalkeeping_handling', 'goalkeeping_kicking',
                    'goalkeeping_positioning', 'goalkeeping_reflexes', 'goalkeeping_speed', 'ls', 'st', 'rs', 'lw', 'lf', 'cf', 'rf', 'rw',
                    'lam', 'cam', 'ram', 'lm', 'lcm', 'cm', 'rcm', 'rm', 'lwb', 'ldm', 'cdm', 'rdm', 'rwb', 'lb', 'lcb', 'cb', 'rcb', 'rb', 'gk')

    def _extract_hive(self, tb_name: str):
        return self.spark.read.table(tb_name).localCheckpoint()

    def transform(self,):
        self.logger.info('Transforming...')
        self.dim_player = self._transform_player()
        self.dim_club = self._transform_club()
        self.dim_nationality = self._transform_nationality()
        self.information = self._transform_infomation()

    def _transform_date(self):
        beign_date = '1750-01-01'
        df = self.spark.sql(
            f"select explode(sequence(to_date('{beign_date}'), to_date('{self.exec_date}'), interval 1 day)) as date")
        df = df.withColumn('date_id',
                           (year('date') * 10000 + month('date') * 100 + dayofmonth('date')).cast(IntegerType())) \
            .withColumn('year', year('date')) \
            .withColumn('month', year('date')) \
            .withColumn('day', year('date'))

        return df

    def _transform_player(self):
        # rename column id
        self.player_df = self.player_df \
            .withColumn('player_id', col('sofifa_id').cast(IntegerType())) \
            .withColumn('gender', col('gender').cast(BooleanType())) \
            .drop('sofifa_id') \
            .drop_duplicates() \
            .na.drop('all')

        if self.spark.catalog.tableExists(tableName=PLAYER, dbName='fifa'):
            src_player = self._extract_hive(tb_name=f'fifa.{PLAYER}')
            return self.merge_into(source_table=src_player, new_table=self.player_df, p_key='player_id')
        return self.player_df

    def _transform_club(self):
        # rename column id
        self.club_df = self.club_df \
            .withColumn('club_id', col('club_team_id').cast(IntegerType())) \
            .drop('club_team_id') \
            .drop_duplicates() \
            .na.drop('all')

        if self.spark.catalog.tableExists(tableName=CLUB, dbName='fifa'):
            src_club = self._extract_hive(tb_name=f'fifa.{CLUB}')
            return self.merge_into(source_table=src_club, new_table=self.club_df, p_key='club_id')
        return self.club_df

    def _transform_nationality(self):
        self.nationality_df = self.nationality_df \
            .withColumn('nationality_id', col('nationality_id').cast(IntegerType())) \
            .drop_duplicates() \
            .na.drop('all')

        if self.spark.catalog.tableExists(tableName=PLAYER, dbName='fifa'):
            src_nationality = self._extract_hive(tb_name=f'fifa.{NATIONALITY}')
            return self.merge_into(source_table=src_nationality, new_table=self.nationality_df, p_key='nationality_id')

        return self.nationality_df

    def _transform_infomation(self):
        information_trans_df = self.infomation_df \
            .withColumn('overall', col('overall').cast(IntegerType())) \
            .withColumn('player_positions', split(upper(col('player_positions')), ', ')) \
            .withColumn('potential', col('potential').cast(IntegerType())) \
            .withColumn('value_eur', col('value_eur').cast(IntegerType())) \
            .withColumn('wage_eur', col('wage_eur').cast(IntegerType())) \
            .withColumn('age', col('age').cast(IntegerType())) \
            .withColumn('height_cm', col('height_cm').cast(IntegerType())) \
            .withColumn('weight_kg', col('weight_kg').cast(IntegerType())) \
            .withColumn('league_level', col('league_level').cast(IntegerType())) \
            .withColumn('club_jersey_number', col('club_jersey_number').cast(IntegerType())) \
            .withColumn('club_contract_valid_until', col('club_contract_valid_until').cast(IntegerType())) \
            .withColumn('nation_jersey_number', col('nation_jersey_number').cast(IntegerType())) \
            .withColumn('preferred_foot', upper(col('preferred_foot'))) \
            .withColumn('weak_foot', col('weak_foot').cast(IntegerType())) \
            .withColumn('skill_moves', col('skill_moves').cast(IntegerType())) \
            .withColumn('international_reputation', col('international_reputation').cast(IntegerType())) \
            .withColumn('work_rate', upper(col('work_rate'))) \
            .withColumn('body_type', upper(regexp_replace(col('body_type'), '\s*\(\d+[\+\-]\d*\)', ''))) \
            .withColumn('player_tags', split(col('player_tags'), ', ')) \
            .withColumn('player_traits', split(col('player_traits'), ', ')) \
            .withColumn('pace', col('pace').cast(IntegerType())) \
            .withColumn('shooting', col('shooting').cast(IntegerType())) \
            .withColumn('passing', col('passing').cast(IntegerType())) \
            .withColumn('dribbling', col('dribbling').cast(IntegerType())) \
            .withColumn('defending', col('defending').cast(IntegerType())) \
            .withColumn('physic', col('physic').cast(IntegerType())) \
            .withColumn('attacking_crossing', when(col('attacking_crossing').isNull(), 0)
                        .otherwise(col('attacking_crossing').cast(IntegerType()))) \
            .withColumn('attacking_finishing', when(col('attacking_finishing').isNull(), 0)
                        .otherwise(col('attacking_finishing').cast(IntegerType()))) \
            .withColumn('attacking_heading_accuracy', when(col('attacking_heading_accuracy').isNull(), 0)
                        .otherwise(col('attacking_heading_accuracy').cast(IntegerType()))) \
            .withColumn('attacking_short_passing', when(col('attacking_short_passing').isNull(), 0)
                        .otherwise(col('attacking_short_passing').cast(IntegerType()))) \
            .withColumn('attacking_volleys', when(col('attacking_volleys').isNull(), 0)
                        .otherwise(col('attacking_volleys').cast(IntegerType()))) \
            .withColumn('skill_dribbling', when(col('skill_dribbling').isNull(), 0)
                        .otherwise(col('skill_dribbling').cast(IntegerType()))) \
            .withColumn('skill_curve', when(col('skill_curve').isNull(), 0)
                        .otherwise(col('skill_curve').cast(IntegerType()))) \
            .withColumn('skill_fk_accuracy', when(col('skill_fk_accuracy').isNull(), 0)
                        .otherwise(col('skill_fk_accuracy').cast(IntegerType()))) \
            .withColumn('skill_long_passing', when(col('skill_long_passing').isNull(), 0)
                        .otherwise(col('skill_long_passing').cast(IntegerType()))) \
            .withColumn('skill_ball_control', when(col('skill_ball_control').isNull(), 0)
                        .otherwise(col('skill_ball_control').cast(IntegerType()))) \
            .withColumn('movement_acceleration', when(col('movement_acceleration').isNull(), 0)
                        .otherwise(col('attacking_finishing').cast(IntegerType()))) \
            .withColumn('movement_sprint_speed', when(col('movement_sprint_speed').isNull(), 0)
                        .otherwise(col('movement_sprint_speed').cast(IntegerType()))) \
            .withColumn('movement_agility', when(col('movement_agility').isNull(), 0)
                        .otherwise(col('movement_agility').cast(IntegerType()))) \
            .withColumn('movement_reactions', when(col('movement_reactions').isNull(), 0)
                        .otherwise(col('movement_reactions').cast(IntegerType()))) \
            .withColumn('movement_balance', when(col('movement_balance').isNull(), 0)
                        .otherwise(col('movement_balance').cast(IntegerType()))) \
            .withColumn('power_shot_power', when(col('power_shot_power').isNull(), 0)
                        .otherwise(col('power_shot_power').cast(IntegerType()))) \
            .withColumn('power_jumping', when(col('power_jumping').isNull(), 0)
                        .otherwise(col('power_jumping').cast(IntegerType()))) \
            .withColumn('power_stamina', when(col('power_stamina').isNull(), 0)
                        .otherwise(col('power_stamina').cast(IntegerType()))) \
            .withColumn('power_strength', when(col('power_strength').isNull(), 0)
                        .otherwise(col('power_strength').cast(IntegerType()))) \
            .withColumn('power_long_shots', when(col('power_long_shots').isNull(), 0)
                        .otherwise(col('power_long_shots').cast(IntegerType()))) \
            .withColumn('mentality_aggression', when(col('mentality_aggression').isNull(), 0)
                        .otherwise(col('mentality_aggression').cast(IntegerType()))) \
            .withColumn('mentality_interceptions', when(col('mentality_interceptions').isNull(), 0)
                        .otherwise(col('mentality_interceptions').cast(IntegerType()))) \
            .withColumn('mentality_positioning', when(col('mentality_positioning').isNull(), 0)
                        .otherwise(col('mentality_positioning').cast(IntegerType()))) \
            .withColumn('mentality_vision', when(col('mentality_vision').isNull(), 0)
                        .otherwise(col('mentality_vision').cast(IntegerType()))) \
            .withColumn('mentality_penalties', when(col('mentality_penalties').isNull(), 0)
                        .otherwise(col('mentality_penalties').cast(IntegerType()))) \
            .withColumn('mentality_composure', when(col('mentality_composure').isNull(), 0)
                        .otherwise(col('mentality_composure').cast(IntegerType()))) \
            .withColumn('defending_marking_awareness', when(col('defending_marking_awareness').isNull(), 0)
                        .otherwise(col('defending_marking_awareness').cast(IntegerType()))) \
            .withColumn('defending_standing_tackle', when(col('defending_standing_tackle').isNull(), 0)
                        .otherwise(col('defending_standing_tackle').cast(IntegerType()))) \
            .withColumn('defending_sliding_tackle', when(col('defending_sliding_tackle').isNull(), 0)
                        .otherwise(col('defending_sliding_tackle').cast(IntegerType()))) \
            .withColumn('goalkeeping_diving', when(col('goalkeeping_diving').isNull(), 0)
                        .otherwise(col('goalkeeping_diving').cast(IntegerType()))) \
            .withColumn('goalkeeping_handling', when(col('goalkeeping_handling').isNull(), 0)
                        .otherwise(col('goalkeeping_handling').cast(IntegerType()))) \
            .withColumn('goalkeeping_kicking', when(col('goalkeeping_kicking').isNull(), 0)
                        .otherwise(col('goalkeeping_kicking').cast(IntegerType()))) \
            .withColumn('goalkeeping_positioning', when(col('goalkeeping_positioning').isNull(), 0)
                        .otherwise(col('goalkeeping_positioning').cast(IntegerType()))) \
            .withColumn('goalkeeping_reflexes', when(col('goalkeeping_reflexes').isNull(), 0)
                        .otherwise(col('goalkeeping_reflexes').cast(IntegerType()))) \
            .withColumn('goalkeeping_speed', when(col('goalkeeping_speed').isNull(), 0)
                        .otherwise(col('goalkeeping_speed').cast(IntegerType()))) \
            .withColumn('goalkeeping_diving', when(col('goalkeeping_diving').isNull(), 0)
                        .otherwise(col('goalkeeping_diving').cast(IntegerType()))) \
            .withColumn('ls', when(col('ls').isNull(), 0)
                        .otherwise(self._calc_point(col('ls')))) \
            .withColumn('st', when(col('st').isNull(), 0)
                        .otherwise(self._calc_point(col('st')))) \
            .withColumn('rs', when(col('rs').isNull(), 0)
                        .otherwise(self._calc_point(col('rs')))) \
            .withColumn('lw', when(col('lw').isNull(), 0)
                        .otherwise(self._calc_point(col('lw')))) \
            .withColumn('lf', when(col('lf').isNull(), 0)
                        .otherwise(self._calc_point(col('lf')))) \
            .withColumn('cf', when(col('cf').isNull(), 0)
                        .otherwise(self._calc_point(col('cf')))) \
            .withColumn('rf', when(col('rf').isNull(), 0)
                        .otherwise(self._calc_point(col('rf')))) \
            .withColumn('rw', when(col('rw').isNull(), 0)
                        .otherwise(self._calc_point(col('rw')))) \
            .withColumn('lam', when(col('lam').isNull(), 0)
                        .otherwise(self._calc_point(col('lam')))) \
            .withColumn('cam', when(col('cam').isNull(), 0)
                        .otherwise(self._calc_point(col('cam')))) \
            .withColumn('ram', when(col('ram').isNull(), 0)
                        .otherwise(self._calc_point(col('ram')))) \
            .withColumn('lm', when(col('lm').isNull(), 0)
                        .otherwise(self._calc_point(col('lm')))) \
            .withColumn('lcm', when(col('lcm').isNull(), 0)
                        .otherwise(self._calc_point(col('lcm')))) \
            .withColumn('cm', when(col('cm').isNull(), 0)
                        .otherwise(self._calc_point(col('cm')))) \
            .withColumn('rcm', when(col('rcm').isNull(), 0)
                        .otherwise(self._calc_point(col('rcm')))) \
            .withColumn('rm', when(col('rm').isNull(), 0)
                        .otherwise(self._calc_point(col('rm')))) \
            .withColumn('lwb', when(col('lwb').isNull(), 0)
                        .otherwise(self._calc_point(col('lwb')))) \
            .withColumn('ldm', when(col('ldm').isNull(), 0)
                        .otherwise(self._calc_point(col('ldm')))) \
            .withColumn('cdm', when(col('cdm').isNull(), 0)
                        .otherwise(self._calc_point(col('cdm')))) \
            .withColumn('rdm', when(col('rdm').isNull(), 0)
                        .otherwise(self._calc_point(col('rdm')))) \
            .withColumn('rwb', when(col('rwb').isNull(), 0)
                        .otherwise(self._calc_point(col('rwb')))) \
            .withColumn('lb', when(col('lb').isNull(), 0)
                        .otherwise(self._calc_point(col('lb')))) \
            .withColumn('lcb', when(col('lcb').isNull(), 0)
                        .otherwise(self._calc_point(col('lcb')))) \
            .withColumn('cb', when(col('cb').isNull(), 0)
                        .otherwise(self._calc_point(col('cb')))) \
            .withColumn('rcb', when(col('rcb').isNull(), 0)
                        .otherwise(self._calc_point(col('rcb')))) \
            .withColumn('rb', when(col('rb').isNull(), 0)
                        .otherwise(self._calc_point(col('rb')))) \
            .withColumn('gk', when(col('gk').isNull(), 0)
                        .otherwise(self._calc_point(col('gk')))) \
            .withColumn('year_version', lit(self.year_version)) \
            .withColumn('player_id', col('sofifa_id').cast(IntegerType())) \
            .withColumn('nationality_id', col('nationality_id').cast(IntegerType())) \
            .withColumn('nation_team_id', col('nation_team_id').cast(IntegerType())) \
            .withColumn('club_id', col('club_team_id').cast(IntegerType())) \
            .withColumn('club_joined_date_id', (year('club_joined') * 10000 + month('club_joined') * 100 + dayofmonth('club_joined')).cast(IntegerType())) \
            .drop('sofifa_id', 'club_team_id', 'club_joined')

        return information_trans_df

    @staticmethod
    @udf(returnType=IntegerType())
    def _calc_point(raw_point: str):
        points = re.split(r'\s*[\+\-]\s*', raw_point)
        if '+' in raw_point:
            return int(points[0]) + int(points[1])
        elif '-' in raw_point:
            return int(points[0]) - int(points[1])
        else:
            return int(points[0])

    def load(self):
        self.logger.info('Loading...')
        self._load(self.dim_player, tb_name=PLAYER, save_mode='overwrite')
        self._load(self.dim_club, tb_name=CLUB, save_mode='overwrite')
        self._load(self.dim_nationality, tb_name=NATIONALITY,
                   save_mode='overwrite')
        self._load(self.information, tb_name=INFORMATION,
                   partition_keys=['year_version'])

    def _load(self, df: DataFrame, tb_name: str, save_mode: str = 'append', partition_keys: list = []):
        self.logger.info('Loading...')
        row_exec = df.select(count('*')).collect()[0][0]
        df.write.option('header', True) \
            .format('hive') \
            .partitionBy(partition_keys) \
            .mode(save_mode) \
            .saveAsTable(f'fifa.{tb_name}')
        self.logger.info(
            f'Load data success in DWH: fifa.{tb_name} with {row_exec} row(s).')

    def run(self):
        self.extract()
        self.transform()
        self.load()

    def run_init(self):
        dim_date = self._transform_date()
        self._load(dim_date, DATE, save_mode='overwrite')
