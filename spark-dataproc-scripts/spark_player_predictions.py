import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import date, timedelta


parser = argparse.ArgumentParser()
parser.add_argument('--season_id', required=True)
parser.add_argument('--day_offset', required=False)

args = parser.parse_args()

season_id = args.season_id
offset_days = args.day_offset

log = date.today()-timedelta(days=1+offset_days)
print(log)

spark = SparkSession.builder \
    .appName('spark-nba') \
    .getOrCreate()

teams = spark.read.format('bigquery') \
  .option('table', 'dez-nba-analytics.nba_database.nba_teams') \
  .load()

teams = teams.select('id', 'full_name', 'abbreviation')

box_df = spark.read.format('bigquery') \
    .option('table', 'dez-nba-analytics.nba_database.player_boxscore') \
    .load() \
    .filter(f.col('season_id') == season_id)

box_df = box_df.join(teams, box_df.team == teams.abbreviation, 'inner')


box_df.createOrReplaceTempView('box_df')

cummulatives_df = spark.sql("""
SELECT
        game_date,
        game_id,
        season_type,
        season_id,
        id AS team_id,
        full_name AS team_name,
        player_id,
        player,
        pts AS points_scored,
        (SUM(PTS) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(game_id) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_pts,
        (SUM(field_goal_made) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(field_goal_made) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_fg_made,
        (SUM(field_goal_attempt) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(field_goal_attempt) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_fg_attempt,
        (SUM(free_throw_made) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(free_throw_made) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_ft_made,
        (SUM(free_throw_attempt) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(free_throw_attempt) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_ft_attempt,
        (SUM(OREB) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(OREB) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_oreb,
        (SUM(DREB) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(DREB) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_dreb,
        (SUM(STL) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(STL) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_stl,
        (SUM(AST) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(AST) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_ast,
        (SUM(BLK) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(BLK) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_blk,
        (SUM(PF) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(PF) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_pf,
        (SUM(TOV) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)/COUNT(TOV) OVER(PARTITION BY season_id, player_id ORDER BY game_date, game_id ROWS UNBOUNDED PRECEDING)) AS cumavg_tov
    FROM
        box_df
""")


cummulatives_df = cummulatives_df.filter(f.col('game_date') == log)


cummulatives_df = cummulatives_df \
    .withColumn('pts_prediction', \
                f.col('cumavg_pts') + \
                0.4*f.col('cumavg_fg_made') - \
                0.7*f.col('cumavg_fg_attempt') - \
                0.4*(f.col('cumavg_ft_attempt') - f.col('cumavg_ft_made')) + \
                0.7*f.col('cumavg_oreb') + \
                0.3*f.col('cumavg_dreb') + \
                f.col('cumavg_stl') + \
                0.7*f.col('cumavg_ast') + \
                0.7*f.col('cumavg_blk') + \
                0.4*f.col('cumavg_pf') - \
                f.col('cumavg_tov') \
               )     

cummulatives_df.write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'dataproc-temp-us-central1-385360674362-ioatwhvx') \
    .mode('append') \
    .save('dez-nba-analytics.nba_database.player_points_prediction')

