if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    df = pd.DataFrame(data)
    game_ids = data['GAME_ID'].unique()
    data = {}
    for match_id in game_ids:

        match_df = df[df['GAME_ID']==match_id]
        season_id = 22023
        team_id_home = match_df['TEAM_ID'][match_df['MATCHUP'].str.contains("vs.", regex=True)]
        team_id_away = match_df['TEAM_ID'][match_df['MATCHUP'].str.contains("vs.", regex=True)]
        


    # Specify your transformation logic here

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'