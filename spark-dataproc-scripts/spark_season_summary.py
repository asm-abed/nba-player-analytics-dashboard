import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import date, timedelta

parser = argparse.ArgumentParser()
parser.add_argument('--season_id', required=True)

args = parser.parse_args()

season_id = args.season_id

spark = SparkSession.builder \
    .appName('spark-nba') \
    .getOrCreate()

summary = spark.read.format('bigquery') \
    .option('table', 'dez-nba-analytics.nba_database.season_player_summary') \
    .load() \
    .filter(f.col('season_id') != season_id)


summary.write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'dataproc-temp-us-central1-385360674362-ioatwhvx') \
    .mode('overwrite') \
    .save('dez-nba-analytics.nba_database.season_player_summary')

box_df = spark.read.format('bigquery') \
    .option('table', 'dez-nba-analytics.nba_database.player_boxscore') \
    .load() \
    .filter(f.col('season_id') == season_id)

box_df.createOrReplaceTempView('box_df')

season_stats = spark.sql("""
SELECT
        -- Basic info about the players
        season_id,
        player_id,
        player AS player_name,
        
        -- Summary of statistics
        SUM(PTS) AS total_points_scored,
        SUM(field_goal_made) AS total_fg_made,
        SUM(field_goal_attempt) AS total_fg_attempts,
        SUM(field_goal_made)/SUM(field_goal_attempt) AS season_shots_percentage,
        SUM(three_pt_made) AS total_three_pt_made,
        SUM(three_pt_attempt) AS total_three_pt_attempts,
        SUM(three_pt_made)/SUM(three_pt_attempt) AS season_three_pt_percentage,
        SUM(free_throw_made) AS total_free_throw_made,
        SUM(free_throw_attempt) AS total_free_throw_attempts,
        SUM(free_throw_made)/SUM(free_throw_attempt) AS season_freethrow_percentage,
        SUM(REB) AS total_rebounds,
        SUM(AST) AS total_assists,
        SUM(STL) AS total_steals,
        SUM(TOV) AS total_turnovers,
        
        -- General averages
        AVG(PTS) AS avg_points_scored_per_match,
        AVG(field_goal_made) AS avg_shots_per_match,
        AVG(three_pt_made) AS avg_three_pt_shots_per_match
    FROM
        box_df
    GROUP BY
        1, 2, 3        
""")

season_stats.write \
    .format('bigquery') \
    .option('temporaryGcsBucket', 'dataproc-temp-us-central1-385360674362-ioatwhvx') \
    .mode('append') \
    .save('dez-nba-analytics.nba_database.season_player_summary')

