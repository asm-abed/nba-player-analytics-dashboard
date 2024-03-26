import pyarrow as pa
import pyarrow.parquet as pq
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp_srv/nba_gcp_credentials.json"

bucket_name = 'dez-nba-datalake-match_linescores'
project_id = 'dez-nba-analytics'

@data_exporter
def export_data(data, *args, **kwargs):
    data['match_date'] = data['game_date'].dt.date

    seasons = data['game_date'].dt.year.unique()
    gcs = pa.fs.GcsFileSystem()

    for year in seasons:
        year_df = data[data['game_date'].dt.year == year]

        for month in range(1,13):
            month_df = year_df[year_df['game_date'].dt.month == month]

            print(f'uploading data from {month:02d}/{year}')

            table = pa.Table.from_pandas(month_df)

            root_path = f'{bucket_name}/{year}/{month:02d}'

            pq.write_to_dataset(
                table,
                root_path=root_path,
                partition_cols=['match_date'],
                filesystem=gcs
            )

        print(f'NBA Season {year} successfully ingested')
    # Specify your data exporting logic here


