from google.cloud import bigquery

class Loader:
    def __init__(self, bq_client, gcs_client):
        """Instantiates the BigQuery Loader class for loading data from storage buckets to BigQuery tables
        
        Args:
            bq_client: BigQuery client
            gcs_client: Google Cloud Storage client
        """
        self.bq_client = bq_client
        self.gcs_client = gcs_client

    def load_from_bucket(self, source_bucket_name, destination_bucket_name, dataset_id, table_id):
        """Loads data from Google Cloud Storage to BigQuery. Expected file format is NDJSON

        Args:
            source_bucket_name: source bucket name
            destination_bucket_name: destination bucket name
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
        """
        # configure BQ details
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        dataset_ref = self.bq_client.dataset(dataset_id)

        # configure GCS details
        print('source bucket: {}'.format(source_bucket_name))
        source_bucket = self.gcs_client.get_bucket(source_bucket_name)
        print('destination bucket: {}'.format(destination_bucket_name))
        destination_bucket = self.gcs_client.get_bucket(destination_bucket_name)

        # list files in source bucket
        for blob in source_bucket.list_blobs():
            filename = blob.name
            print("found file: {}".format(filename))
            file_uri = "gs://{}/{}".format(source_bucket_name, filename)

            # load file to BQ
            load_job = self.bq_client.load_table_from_uri(file_uri, dataset_ref.table(table_id), job_config=job_config)
            print("starting job {}".format(load_job.job_id))
            load_job.result()
            destination_table = self.bq_client.get_table(dataset_ref.table(table_id))
            print("table {} has {} rows".format(destination_table.table_id, destination_table.num_rows))

            # transfer file to processed bucket
            source_blob = source_bucket.blob(filename)
            destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, filename)
            print("transfered file to processed bucket: {}".format(filename))

            # delete file from staging bucket
            source_blob.delete()
            print("deleted file from staging bucket: {}".format(filename))

        return "Completed loading files to BigQuery", 200