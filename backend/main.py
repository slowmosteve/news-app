import os
import json
import time
import datetime
import gcsfs
import requests
import uuid
from flask import Flask, request
from subscriber import Subscriber
from loader import Loader
from google.cloud import pubsub, bigquery, storage
import google.auth
from google.auth import impersonated_credentials

app = Flask(__name__)

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
gcsfs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)

@app.route('/', methods=['GET'])
def index():
    return ('Backend server running', 200)

@app.route('/get_news', methods=['GET'])
def get_news():

    credentials, gcp_project_id = google.auth.default()
    gcs_client = storage.Client(credentials=credentials)

    api_key = os.getenv('NEWS_API_KEY')
    url_base = 'https://newsapi.org'
    url_path = '/v2/top-headlines'
    url_params = {
        'language': 'en',
        'apiKey': api_key,
        'pageSize': 100 
        }
    print('requesting news for endpoint: {}, params: {}'.format(url_path, url_params))

    url_params['apiKey'] = api_key
    request_url = str(url_base + url_path)
    response = requests.get(request_url, params=url_params)

    print('status: '+str(response.json()['status']))

    articles = []
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    filename_time = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    for i in range(len(response.json()['articles'])):
        # create empty list of article details
        details = {}

        # populate fields from news results
        result = response.json()['articles'][i]
        # print('\n{}\n'.format(result))
        columns = ['title','author','description','content','url','urlToImage','publishedAt']
        details['article_id'] = str(uuid.uuid4())
        details['article_order'] = i
        details['load_timestamp'] = current_time
        for column in columns:
            details[column] = result[column]
        articles.append(details)

    bucket_path = os.getenv('ARTICLES_BUCKET')
    filename = 'news-{}'.format(filename_time)

    output_file = gcsfs.open("{}/{}.ndjson".format(bucket_path, filename), 'w')
    
    for item in articles:
        output_file.write(json.dumps(item))
        output_file.write('\n')

    print('wrote file {}.ndjson to bucket'.format(filename))

    return "Retrieved news data", 200

@app.route('/load_news', methods=['GET'])
def load_news():
    """This route will load news files in the storage bucket to the BigQuery tables 
    """
    credentials, gcp_project_id = google.auth.default()
    gcs_client = storage.Client(project=gcp_project_id, credentials=credentials)
    bigquery_client = bigquery.Client(project=gcp_project_id, credentials=credentials)

    # instantiate Loader class and load file to BigQuery
    loader = Loader(bigquery_client, gcs_client)
    dataset_id = 'news'

    articles_bucket = os.getenv('ARTICLES_BUCKET')
    articles_processed_bucket = os.getenv('ARTICLES_PROCESSED_BUCKET')
    articles_table_id = 'articles'
    articles_load_job = loader.load_from_bucket(articles_bucket, articles_processed_bucket, dataset_id, articles_table_id)

    return "Loaded news data to BigQuery", 200

@app.route('/get_messages', methods=['GET'])
def fetch_data():
    """This route will retrieve messages from the Pubsub topic
    """
    credentials, gcp_project_id = google.auth.default()
    # instantiate a pubsub subscriber client and subscriber class
    subscriber_client = pubsub.SubscriberClient(credentials=credentials)
    subscriber = Subscriber(subscriber_client, gcsfs)

    # use current time for filenames
    current_time = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    # subscribe to impressions topic to retrieve messages and write to bucket
    print("""
        *********************
        processing impressions
        *********************
    """
    )
    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    subscription_path = subscriber_client.subscription_path(gcp_project_id, 'news_impressions')
    impressions_filename = 'impression-{}'.format(current_time)
    impressions_messages = subscriber.get_messages(subscription_path, impressions_bucket, impressions_filename)

    # subscribe to clicks topic to retrieve messages and write to bucket
    print("""
        *********************
        processing clicks
        *********************
    """
    )
    clicks_bucket = os.getenv('CLICKS_BUCKET')
    subscription_path = subscriber_client.subscription_path(gcp_project_id, 'news_clicks')
    clicks_filename = 'clicks-{}'.format(current_time)
    clicks_messages = subscriber.get_messages(subscription_path, clicks_bucket, clicks_filename)

    # return impressions_messages
    return "Pulled tracking messages from topic", 200

@app.route('/load_tracking', methods=['GET'])
def load_tracking():
    """This route will load tracking files in the storage bucket to the BigQuery tables 
    """
    credentials, gcp_project_id = google.auth.default()
    # instantiate a bigquery client
    bigquery_client = bigquery.Client(project=gcp_project_id, credentials=credentials)
    gcs_client = storage.Client(project=gcp_project_id, credentials=credentials)

    loader = Loader(bigquery_client, gcs_client)
    dataset_id = 'tracking'

    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    impressions_processed_bucket = os.getenv('IMPRESSIONS_PROCESSED_BUCKET')
    impressions_table_id = 'impressions'
    impressions_load_job = loader.load_from_bucket(impressions_bucket, impressions_processed_bucket, dataset_id, impressions_table_id)

    clicks_bucket = os.getenv('CLICKS_BUCKET')
    clicks_processed_bucket = os.getenv('CLICKS_PROCESSED_BUCKET')
    clicks_table_id = 'clicks'
    clicks_load_job = loader.load_from_bucket(clicks_bucket, clicks_processed_bucket, dataset_id, clicks_table_id)

    return "Loaded tracking data to BigQuery", 200

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8081

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)