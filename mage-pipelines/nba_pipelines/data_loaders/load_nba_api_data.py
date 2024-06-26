if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


from nba_api.live.nba.endpoints import scoreboard
import pandas as pd
import json
from nba_api.stats.endpoints import TeamGameLogs, BoxScoreTraditionalV2 
from datetime import date, timedelta
import numpy as np
import requests

@data_loader
def load_data(*args, **kwargs):
    ## Use nba_api to get current matches
    game_datapull = TeamGameLogs(league_id_nullable ='00', season_nullable = '2023-24')
    games_df = game_datapull.get_data_frames()[0]


    #filter it to today's matches only
    date_today = date.today() - timedelta(days=1)
    print(date_today)
    games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
    games_df = games_df[games_df['GAME_DATE'] ==  np.datetime64(date_today)] 


    game_ids = games_df['GAME_ID'].unique()
  
    df = pd.DataFrame()
    for match_id in game_ids:
        print(match_id)
    
        game = games_df[games_df['GAME_ID']==match_id]

        try:
            match_df = BoxScoreTraditionalV2(end_period=10, end_range=28800, game_id = match_id, range_type = 2, start_period=1, start_range=0).get_data_frames()[0]

            ## to match the schema in bigquery
            home = game['TEAM_ABBREVIATION'][game['MATCHUP'].str.contains("vs", regex=True)]
            away = game['TEAM_ABBREVIATION'][game['MATCHUP'].str.contains("@", regex=True)]
            
            # Repeat values of 'home' and 'away' to match the length of match_df
            home = pd.concat([home] * len(match_df), ignore_index=True)
            away = pd.concat([away] * len(match_df), ignore_index=True)

            # Apply the values of 'home' and 'away' to the entire columns
            match_df['home'] = home
            match_df['away'] = away

            df = pd.concat([df, match_df], axis=0)

        except  Exception as e:
            # Handle the error (print a message, log the error, etc.)
            print(f"An error occurred: {e}")
            # Proceed to the next iteration
            continue

        
    return {'df': df, 'games':games_df}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
