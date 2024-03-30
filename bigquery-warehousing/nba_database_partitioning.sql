CREATE OR REPLACE TABLE
  dez-nba-analytics.nba_database_optimized.player_boxscore_par_cl
PARTITION BY
  DATE_TRUNC(game_date, MONTH)
CLUSTER BY
  season_id AS
SELECT * FROM `dez-nba-analytics.nba_database.player_boxscore`;

CREATE OR REPLACE TABLE
  dez-nba-analytics.nba_database_optimized.player_points_prediction_par_cl
PARTITION BY
  DATE_TRUNC(game_date, MONTH)
CLUSTER BY
  season_id AS
SELECT * FROM `dez-nba-analytics.nba_database.player_points_prediction`;

CREATE OR REPLACE TABLE
  dez-nba-analytics.nba_database_optimized.season_player_summary_par_cl
CLUSTER BY
  season_id AS
SELECT * FROM `dez-nba-analytics.nba_database.season_player_summary`;

