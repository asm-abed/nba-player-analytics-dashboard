CREATE TABLE nba_database.player_boxscore_raw
SELECT * FROM {{ df_1 }};


CREATE TABLE nba_database.player_boxscore
PARTITION BY DATE(game_date)
CLUSTER BY season_id AS
SELECT * FROM player_boxscore_raw;


