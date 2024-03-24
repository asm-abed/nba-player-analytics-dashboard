import pyarrow as pa
import pyarrow.parquet as pq
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/.gc/nba_gcp_credentials.json"

bucket_name = 'dez-nba-datalake-boxscore'
project_id = 'dez-nba-analytics'

@data_exporter
def export_data(data, *args, **kwargs):

    seasons = data['season'].unique()
    print(seasons)

    data['match_date'] = data['date'].dt.date

    gcs = pa.fs.GcsFileSystem()

    for year in seasons:
        data_iter = data[data['season']==year]

        table = pa.Table.from_pandas(data_iter)

        root_path = f'{bucket_name}/{year}'

        pq.write_to_dataset(
            table,
            root_path=root_path,
            partition_cols=['match_date'],
            filesystem=gcs
        )

        print(f'NBA Season {year} successfully ingested')
    # Specify your data exporting logic here


