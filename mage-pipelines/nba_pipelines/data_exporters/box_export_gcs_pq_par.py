import pyarrow as pa
import pyarrow.parquet as pq
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp_srv/nba_gcp_credentials.json"

bucket_name = 'dez-nba-datalake'
folder_name = 'player_boxscore'
project_id = 'dez-nba-analytics'

@data_exporter
def export_data(data, *args, **kwargs):
    data['game_date'] = pd.to_datetime(data['game_date'])

    data['month'] = data['game_date'].dt.month

    seasons = data['game_date'].dt.year.unique()
    gcs = pa.fs.GcsFileSystem()

    for year in seasons:
        year_df = data[data['game_date'].dt.year == year]

        print(f'uploading data from {year}')

        table = pa.Table.from_pandas(year_df)

        root_path = f'{bucket_name}/{folder_name}/{year}'

        pq.write_to_dataset(
            table,
            root_path=root_path,
            partition_cols=['month'],
            filesystem=gcs
        )

        print(f'NBA Season {year} successfully ingested')
    # Specify your data exporting logic here


