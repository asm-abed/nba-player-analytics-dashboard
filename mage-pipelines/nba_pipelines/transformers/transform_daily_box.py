if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@transformer
def transform(data,data_2, *args, **kwargs):
    df = pd.DataFrame(data_2)
    bq_latest_date = data

    df = df[df['game_date']  > bq_latest_date]



    df['game_id'] = df['game_id'].astype(int)

    df = df.drop(columns = ['month'])




    return df


@test
def test_output(output, *args) -> None:
    assert len(output['game_id'].isnull())>0, 'Nulls in game ID'
    assert output is not None, 'The output is undefined'
