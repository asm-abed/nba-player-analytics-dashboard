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
    date_today = date.today() - timedelta(days=2)
    print(date_today)
    games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
    
    games_df = games_df[games_df['GAME_DATE'] <  np.datetime64(date_today)] 
    # Specify your data loading logic here

    return games_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'