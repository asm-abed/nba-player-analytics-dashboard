if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    df = pd.DataFrame(data)

    df.date = pd.to_datetime(df.date)

    df.rename(columns={
        'MIN': 'mins_played',
        'FGM': 'field_goal_made',
        'FGA': 'field_goal_attempt',
        'FG%': 'field_goal_pct',
        '3PM': 'three_pt_made',
        '3PA': 'three_pt_attempt',
        '3P%': 'three_pt_pct',
        'FTM': 'free_throw_made',
        'FTA': 'free_throw_attempt',
        'FT%': 'free_throw_pct',
        '+/-': 'plusminus',
        'type': 'season_type',
        'season': 'season_id',
        'gameid': 'game_id',
        'date':'game_date',
        'playerid':'player_id'
        }, inplace=True)

    df['field_goal_pct'] = df['field_goal_pct']/100
    df['three_pt_pct'] = df['three_pt_pct']/100
    df['free_throw_pct']= df['free_throw_pct']/100

    df.loc[~df['season_type'].isin(['regular', 'playoff']), 'season_type'] = 'Others'
    df.loc[df['season_type'] == 'regular', 'season_type'] = 'Regular Season'
    df.loc[df['season_type'] == 'playoff', 'season_type'] = 'Playoffs'

    return df


@test
def test_output(output, *args) -> None:
    assert len(output['game_id'].isnull())>0, 'Output has invalid nulls'
    assert output is not None, 'The output is undefined'
