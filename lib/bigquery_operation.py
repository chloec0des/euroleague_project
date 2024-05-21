
from google.cloud import bigquery

def load_csv_to_bigquery(csv_file_path, dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True  # Automatically detect schema
    job_config.skip_leading_rows = 1  # Skip header row

    with open(csv_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete

    print("Loaded data into BigQuery")

# Example usage
load_csv_to_bigquery('dataeuro/shot_data.csv', 'keen-genre-365508.euroleague_data', 'keen-genre-365508.euroleague_data.shot_data')
