if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    df = pd.DataFrame(data)

    ##removing rows with descriptions about the start and end of quarters
    df = df[df['neutraldescription'].isnull()]

    ## adding columns based from existing ones so analysis later on would be easier
    df['clock_min'] = df['pctimestring'].str.split(":").str[0].astype(int)
    df['clock_sec'] = df['pctimestring'].str.split(":").str[1].astype(int)
    df['home_score'] = df['score'].str.split("-").str[0].astype(int)
    df['away_score'] = df['score'].str.split("-").str[1].astype(int)
    df['score_margin'] = abs(df['home_score']-df['away_score'])

    ## removing columns i have no use for
    df = df.drop(columns = [
        'eventmsgtype', 'eventmsgactiontype','wctimestring','person1type','person2type',
        'person3type', 'player3_id', 'player3_name', 'player3_team_id', 'player3_team_city', 
        'player3_team_nickname', 'player3_team_abbreviation', 'video_available_flag', 'neutraldescription',
        'player1_team_city','player1_team_nickname','player2_team_nickname','player2_team_city',
        'player2_team_id','player2_team_abbreviation','score','pctimestring','scoremargin'
        ])

    ## renaming some of the columns for easy comprehension
    df.rename(columns = {
        'period':'quarter',
        'eventnum':'event_id',
        'homedescription':"home_action",
        'visitordescription':'away_action',
        'player1_id':'scorer_id',
        'player1_team_id':'scorer_team_id',
        "player1_name":'scorer_name',
        'player1_team_abbreviation':"scorer_team_abbrev",
        'player2_id':'assist_id',
        'player2_name':'assist_name'
    }, inplace=True)


    df = df[[
        'game_id',
        'event_id',
        'quarter',
        'clock_min',
        'clock_sec',
        'home_action',
        'away_action',
        'home_score',
        'away_score',
        'score_margin',
        'scorer_id',
        'scorer_name',
        'scorer_team_id',
        'scorer_team_abbrev',
        'assist_id',
        'assist_name'
    ]]


    df['scorer_id'] = df['scorer_id'].astype(int)
    df['scorer_team_id'] = df['scorer_team_id'].astype(int)
    df['assist_id'] = df['assist_id'].astype(int)


    return df


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'