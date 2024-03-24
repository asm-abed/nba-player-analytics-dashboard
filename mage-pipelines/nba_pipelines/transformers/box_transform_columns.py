if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    df = pd.DataFrame(data)

    df.date = pd.to_datetime(df.date, format='%Y-%m-%d')

    df.rename(columns={
        'MIN': 'mins-played',
        'FGM': 'field-goal-made',
        'FGA': 'field-goal-attempt',
        'FG%': 'field-goal-pct',
        '3PM': 'three-pt-made',
        '3PA': 'three-pt-attempt',
        '3P%': 'three-pt-pct',
        'FTM': 'free-throw-made',
        'FTA': 'free-throw-attempt',
        'FT%': 'free-throw-pct',
        '+/-': 'plusminus'
        }, inplace=True)

    df['field-goal-pct'] = df['field-goal-pct']/100
    df['three-pt-pct'] = df['three-pt-pct']/100
    df['free-throw-pct']= df['free-throw-pct']/100

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
