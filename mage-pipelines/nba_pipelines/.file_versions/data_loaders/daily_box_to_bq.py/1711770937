from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import GcsFileSystem
import os


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp_srv/nba_gcp_credentials.json"
project_id = 'dez-nba-analytics'

@data_loader
def load_data(*args, **kwargs):
    bucket_name = 'dez-nba-datalake'
    blob_prefix = 'player_boxscore/2024'
    root_path = f"{bucket_name}/{blob_prefix}"    
    pa_table = pq.read_table(
        source=root_path,
        filesystem=GcsFileSystem(),        
    )

    return pa_table.to_pandas()



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
