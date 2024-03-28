import pyarrow as pa
import pyarrow.parquet as pq
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp_srv/nba_gcp_credentials.json"

bucket_name = 'dez-nba-datalake'
folder_name = 'scoring_plays'
project_id = 'dez-nba-analytics'

@data_exporter
def export_data(data, *args, **kwargs):
    ## creating a partition column based on the first FIVE characters of the game_id

    data['id_parcol'] = data['game_id'].astype(str).str[:5]

    gcs = pa.fs.GcsFileSystem()

    table = pa.Table.from_pandas(data)

    root_path = f'{bucket_name}/{folder_name}/'

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['id_parcol'],
        filesystem=gcs
    )



