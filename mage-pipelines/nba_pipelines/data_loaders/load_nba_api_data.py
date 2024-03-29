if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from nba_api.live.nba.endpoints import scoreboard
import pandas as pd
import json
from nba_api.stats.endpoints import TeamGameLogs


@data_loader
def load_data(*args, **kwargs):
    game_datapull = TeamGameLogs(league_id_nullable ='00', season_nullable = '2023-24')
    df = df_season = game_datapull.get_data_frames()[0]

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
