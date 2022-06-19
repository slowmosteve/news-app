import os
import json
import time
import datetime
import gcsfs
import logging
import papermill
from flask import Flask, request
from news import News
from subscriber import Subscriber
from loader import Loader
from google.cloud import pubsub, bigquery, storage
import google.auth

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
ENV = os.getenv('ENV')
credentials, gcp_project_id = google.auth.default()
bigquery_client = bigquery.Client(project=gcp_project_id, credentials=credentials)
gcs_client = storage.Client(project=gcp_project_id, credentials=credentials)
subscriber_client = pubsub.SubscriberClient(credentials=credentials)
gcsfs_client = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
loader = Loader(bigquery_client, gcs_client, gcsfs_client)
subscriber = Subscriber(subscriber_client, gcsfs_client)

@app.route('/', methods=['GET'])
def index():
    return ('Backend server running', 200)


@app.route('/get_and_load_news', methods=['POST'])
def get_and_load_news():
    """This route retrieves news data and writes to cloud storage before loading to BigQuery
    """

    logger = logging.getLogger('app.get_and_load_news')
    print('requesting news')
    logger.info('requesting news')

    # Get news data from newsapi
    if ENV=='prod':
        secrets_bucket_name = os.getenv('SECRETS_BUCKET')
        secrets_bucket = gcs_client.get_bucket(secrets_bucket_name)
        secret_blob = secrets_bucket.get_blob('news-api-key.json').download_as_string()
        secret_json = json.loads(secret_blob.decode('utf-8'))
        api_key = secret_json['key']
    else:
        api_key = os.getenv('NEWS_API_KEY')
    
    news_client = News(api_key)
    date_filter = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')
    news_domains = """
        abcnews.go.com, apnews.com, aljazeera.com, axios.com, bbc.co.uk, bloomberg.com, 
        cbc.ca, us.cnn.com, engadget.com, ew.com, espn.go.com, business.financialpost.com, 
        fortune.com, foxnews.com, news.google.com, news.ycombinator.com, ign.com, 
        mashable.com, msnbc.com, mtv.com, nationalgeographic.com, nbcnews.com, 
        newscientist.com, newsweek.com, nymag.com, nextbigfuture.com, polygon.com, 
        reuters.com, techcrunch.com, techradar.com, theglobeandmail.com, 
        huffingtonpost.com, thenextweb.com, theverge.com, wsj.com, washingtonpost.com, 
        time.com, usatoday.com, news.vice.com, wired.com
    """
    news_response = news_client.get_news(date_filter, news_domains)
    formatted_news = news_client.format_articles(news_response)

    # Load to GCS
    bucket_path = os.getenv('ARTICLES_BUCKET')
    loader.load_file_to_bucket(articles=formatted_news, bucket_path=bucket_path)

    # load to BQ
    retries = 3
    count = 1
    articles_load_job_status = None
    while (articles_load_job_status != 200 or count < retries):
        time.sleep(10)
        print('loading news (attempt: {}'.format(count))
        logger.info('loading news (attempt: {}'.format(count))
        dataset_id, articles_table_id = 'news', 'articles'
        articles_bucket = os.getenv('ARTICLES_BUCKET')
        articles_processed_bucket = os.getenv('ARTICLES_PROCESSED_BUCKET')
        articles_load_job_status = loader.load_from_bucket(articles_bucket, articles_processed_bucket, dataset_id, articles_table_id)
        print('loading news status {}'.format(articles_load_job_status[1]))
        logger.info('loading news status {}'.format(articles_load_job_status[1]))
        if articles_load_job_status[1] == 200:
            return 'Retrieved news and loaded data to BigQuery', 200
        count += 1
    return 'Unable to retrieve and load news', 204


@app.route('/get_and_load_tracking', methods=['POST'])
def get_and_load_tracking():
    """This route will retrieve messages from the Pubsub topic and load to BigQuery
    """
    logger = logging.getLogger('app.get_and_load_tracking')
    print('retrieving tracking messages')
    logger.info('retrieving tracking messages')

    # subscribe to impressions topic to retrieve messages and write to bucket
    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    subscription_path = subscriber_client.subscription_path(gcp_project_id, 'news_impressions')
    impressions_filename = 'impression-{}'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
    impressions_messages = subscriber.get_messages(subscription_path, impressions_bucket, impressions_filename)
    print('impressions tracking status: {}'.format(impressions_messages))
    logger.info('impressions tracking status: {}'.format(impressions_messages))

    # subscribe to clicks topic to retrieve messages and write to bucket
    clicks_bucket = os.getenv('CLICKS_BUCKET')
    subscription_path = subscriber_client.subscription_path(gcp_project_id, 'news_clicks')
    clicks_filename = 'clicks-{}'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
    clicks_messages = subscriber.get_messages(subscription_path, clicks_bucket, clicks_filename)
    print('click tracking status: {}'.format(clicks_messages))
    logger.info('click tracking status: {}'.format(clicks_messages))

    dataset_id = 'tracking'
    impressions_table_id, clicks_table_id = 'impressions', 'clicks'
    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    impressions_processed_bucket = os.getenv('IMPRESSIONS_PROCESSED_BUCKET')
    clicks_bucket = os.getenv('CLICKS_BUCKET')
    clicks_processed_bucket = os.getenv('CLICKS_PROCESSED_BUCKET')
    
    retries, count = 3, 1
    clicks_status = None
    while (clicks_status != 200 or count < retries):
        time.sleep(10)
        print('loading impression tracking (attempt: {}'.format(count))
        logger.info('loading impression tracking (attempt: {}'.format(count))
        impressions_load_job = loader.load_from_bucket(impressions_bucket, impressions_processed_bucket, dataset_id, impressions_table_id)
        clicks_load_job = loader.load_from_bucket(clicks_bucket, clicks_processed_bucket, dataset_id, clicks_table_id)
        clicks_status = clicks_load_job[1]
        print('loading tracking status {}'.format(clicks_status))
        logger.info('loading tracking status {}'.format(clicks_status))
        if clicks_status == 200:
            return 'Retrieved tracking and loaded data to BigQuery', 200
        count += 1
    return 'Unable to retrieve and load tracking', 204


@app.route('/get_recommendations', methods=['POST'])
def get_recommendations():
    """This route will run the topic model used to populate the recommended articles for all users
    """
    logger = logging.getLogger('app.get_recommendations')
    print('Updating topic model and recommendations')
    logger.info('Updating topic model and recommendations')

    notebook_bucket = os.getenv('NOTEBOOK_BUCKET')
    run_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    papermill.execute_notebook(
        'gs://{}/prod/topic-prod.ipynb'.format(notebook_bucket),
        'gs://{}/out/topic-out-{}.ipynb'.format(notebook_bucket, run_time),
        kernel_name = 'python3'
    )

    return 'Ran topic model and updated recommendations', 200
    

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8081

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)