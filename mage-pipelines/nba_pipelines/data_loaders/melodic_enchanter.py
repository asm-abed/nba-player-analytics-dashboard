import os
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from pyarrow.fs import GcsFileSystem
import pandas as pd

# Set up your environment
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp_srv/nba_gcp_credentials.json"

# Define the GCS bucket and common prefix
bucket_name = 'nba_raw_source'
prefix = 'play_by_play'

# Initialize GCSFileSystem
fs = GcsFileSystem()

# List files in the GCS bucket with the common prefix
file_list = fs.get_file_info(f"{bucket_name}/{prefix}")
print(file_list)
# Initialize an empty list to store PyArrow tables
tables = []

# Iterate over each file and read CSV into PyArrow table
for file_info in file_list:
    file_path = f"gs://{bucket_name}/{file_info.path}"
    with fs.open_input_stream(file_path) as f:
        table = pcsv.read_csv(f)
        tables.append(table)

# Concatenate PyArrow tables into a single table
combined_table = pq.concat_tables(tables)

# Convert the combined table into a Pandas DataFrame
df = combined_table.to_pandas()

# Now, df contains all the data from the CSV files in the GCS bucket with the common prefix
print(df)
