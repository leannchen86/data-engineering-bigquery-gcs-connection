import pandas as pd
from google.cloud import storage, bigquery
from io import StringIO
import os
# Set your credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'your_service_account_key.json'
bucket_name = "your_bucket_name"
# Create a storage client
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name) # Get the bucket object
blobs = storage_client.list_blobs(bucket_name) # Get the blob object

# Map pandas data types to BigQuery data types
def pandas_type_to_bigquery_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT64'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT64'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOL'
    else:
        return 'STRING'

def get_files_from_gcs():
    file_lst_gcs = []
    # Print the file names
    for blob in blobs:
        file_lst_gcs.append(blob.name)
    
    return file_lst_gcs

def get_schema(file_name):
    
    blob = storage.Blob(file_name, bucket)
    # Read the CSV file content
    csv_content = blob.download_as_text()
    
    # Convert the content to a pandas DataFrame
    data_frame = pd.read_csv(StringIO(csv_content))
    # Create the BigQuery schema
    schema = [
        bigquery.SchemaField(column_name, pandas_type_to_bigquery_type(dtype))
        for column_name, dtype in zip(data_frame.columns, data_frame.dtypes)
    ]
    return schema
