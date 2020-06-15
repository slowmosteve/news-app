import os
import gcsfs
from flask import Flask, request
from subscriber import Subscriber
from loader import Loader
from google.cloud import pubsub, bigquery, storage

app = Flask(__name__)

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
gcsfs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID, token='./.creds/news-site-subscriber.json')

@app.route('/', methods=['GET'])
def index():
    return ('Backend server running', 200)

@app.route('/get_messages', methods=['GET'])
def fetch_data():
    """This route will retrieve messages from the Pubsub topic
    """
    # instantiate a pubsub subscriber client and subscriber class
    subscriber_client = pubsub.SubscriberClient.from_service_account_json('./.creds/news-site-subscriber.json')
    subscriber = Subscriber(subscriber_client, gcsfs)

    # subscribe to impressions topic to retrieve messages and write to bucket
    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    subscription_path = subscriber_client.subscription_path(GCP_PROJECT_ID, 'news_impressions')
    impressions_messages = subscriber.get_messages(subscription_path, impressions_bucket)

    # subscribe to clicks topic to retrieve messages and write to bucket
    clicks_bucket = os.getenv('CLICKS_BUCKET')
    subscription_path = subscriber_client.subscription_path(GCP_PROJECT_ID, 'news_clicks')
    clicks_messages = subscriber.get_messages(subscription_path, clicks_bucket)

    # return impressions_messages
    return "Pulled messages from topic", 200

@app.route('/load_data', methods=['GET'])
def load_data():
    """This route will load files in the storage bucket to the BigQuery tables 
    """
    # instantiate a bigquery client
    bigquery_client = bigquery.Client.from_service_account_json('./.creds/news-site-bq-loader.json')
    gcs_client = storage.Client.from_service_account_json('./.creds/news-site-bq-loader.json')

    loader = Loader(bigquery_client, gcs_client)
    dataset_id = os.getenv('DATASET_ID')

    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    impressions_processed_bucket = os.getenv('IMPRESSIONS_PROCESSED_BUCKET')
    impressions_table_id = os.getenv('IMPRESSIONS_TABLE')
    load_job = loader.load_from_bucket(impressions_bucket, impressions_processed_bucket, dataset_id, impressions_table_id)

    return load_job

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8081

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)