import os
import cloud_storage as gcs
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'your_service_account_key.json'
client = bigquery.Client()
dataset_id = 'lkdata'
dataset_ref = bigquery.DatasetReference(client.project, dataset_id)

table_id_list = gcs.get_files_from_gcs()

table_id_list.remove('p_channel_demographics_a1')

for i in range(len(table_id_list)):
    table_originalId = table_id_list[i]
    table_id = f"{table_originalId}_gcs"

    schema = gcs.get_schema(table_id_list[i])
    table_ref = bigquery.Table(dataset_ref.table(table_id), schema=schema)

    external_config = bigquery.ExternalConfig('CSV')
    source_uri = f"gs://lkdatabase/{table_originalId}"
    external_config.source_uris = [source_uri]
    external_config.csv_options.skip_leading_rows = 1

    table_ref.external_data_configuration = external_config
    table = client.create_table(table_ref)

    sql = 'SELECT * FROM `{0}.{1}`'.format(dataset_id, table_id)

    query_job = client.query(sql)

    output = list(query_job)



