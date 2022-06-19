import os
import json
import time
import datetime
import gcsfs
import requests
import uuid
import logging
import papermill
from flask import Flask, request
from news import News
from subscriber import Subscriber
from loader import Loader
from google.cloud import pubsub, bigquery, storage
import google.auth
from google.auth import impersonated_credentials

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
ENV = os.getenv('ENV')
gcsfs_client = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)

# def get_news():
#     """This function retrieves news data and stores in cloud storage
#     """
#     logger = logging.getLogger('app.get_news')

#     credentials, gcp_project_id = google.auth.default()
#     gcs_client = storage.Client(credentials=credentials)

#     if ENV=='prod':
#         secrets_bucket_name = os.getenv('SECRETS_BUCKET')
#         secrets_bucket = gcs_client.get_bucket(secrets_bucket_name)
#         secret_blob = secrets_bucket.get_blob('news-api-key.json').download_as_string()
#         secret_json = json.loads(secret_blob.decode('utf-8'))
#         api_key = secret_json['key']
#     else:
#         api_key = os.getenv('NEWS_API_KEY')

#     date_filter = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

#     domain_list_string = """
#         abcnews.go.com, apnews.com, aljazeera.com, axios.com, bbc.co.uk, bloomberg.com, 
#         cbc.ca, us.cnn.com, engadget.com, ew.com, espn.go.com, business.financialpost.com, 
#         fortune.com, foxnews.com, news.google.com, news.ycombinator.com, ign.com, 
#         mashable.com, msnbc.com, mtv.com, nationalgeographic.com, nbcnews.com, 
#         newscientist.com, newsweek.com, nymag.com, nextbigfuture.com, polygon.com, 
#         reuters.com, techcrunch.com, techradar.com, theglobeandmail.com, 
#         huffingtonpost.com, thenextweb.com, theverge.com, wsj.com, washingtonpost.com, 
#         time.com, usatoday.com, news.vice.com, wired.com
#     """

#     url_base = 'https://newsapi.org'
#     url_path = '/v2/everything'
#     url_params = {
#         'from': date_filter,
#         'language': 'en',
#         'apiKey': api_key,
#         'pageSize': 100,
#         'sortBy': 'publishedAt',
#         'domains': domain_list_string
#         }
#     print('requesting news for endpoint: {}, params: {}'.format(url_path, url_params))
#     logger.info('requesting news for endpoint: {}, params: {}'.format(url_path, url_params))

#     url_params['apiKey'] = api_key
#     request_url = str(url_base + url_path)
#     response = requests.get(request_url, params=url_params)

#     print('status: {}'.format(str(response.json()['status'])))
#     logger.info('status: {}'.format(str(response.json()['status'])))

#     articles = []
#     current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     filename_time = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
#     # populate article list with details for each article
#     for index, article in enumerate(response.json()['articles']):
#         details = {}
#         # print('\n{}\n'.format(result))
#         details['article_id'] = str(uuid.uuid4())
#         details['article_order'] = index
#         details['load_timestamp'] = current_time
#         for column in ['title', 'author', 'description', 'content', 'url', 'urlToImage', 'publishedAt']:
#             details[column] = article[column]
#         articles.append(details)

#     print('articles: {}'.format(articles))

#     bucket_path = os.getenv('ARTICLES_BUCKET')
#     filename = 'news-{}'.format(filename_time)

#     output_file = gcsfs.open('{}/{}.ndjson'.format(bucket_path, filename), 'w')
    
#     for item in articles:
#         output_file.write(json.dumps(item))
#         output_file.write('\n')

#     print('wrote file {}.ndjson to bucket'.format(filename))
#     logger.info('wrote file {}.ndjson to bucket'.format(filename))

#     return 'Retrieved news data', 200

# def load_news():
#     """This function will load news files in the storage bucket to the BigQuery tables 
#     """
#     logger = logging.getLogger('app.load_news')

#     credentials, gcp_project_id = google.auth.default()
#     gcs_client = storage.Client(project=gcp_project_id, credentials=credentials)
#     bigquery_client = bigquery.Client(project=gcp_project_id, credentials=credentials)

#     # instantiate Loader class and load file to BigQuery
#     loader = Loader(bigquery_client, gcs_client)
#     dataset_id = 'news'

#     articles_bucket = os.getenv('ARTICLES_BUCKET')
#     articles_processed_bucket = os.getenv('ARTICLES_PROCESSED_BUCKET')
#     articles_table_id = 'articles'
#     print('loading news from bucket')
#     logger.info('loading news from bucket')
#     articles_load_job = loader.load_from_bucket(articles_bucket, articles_processed_bucket, dataset_id, articles_table_id)

#     return 'Loaded news data to BigQuery', 200


def get_tracking():
    """This function will retrieve tracking messages from the Pubsub topic
    """
    logger = logging.getLogger('app.get_tracking')

    credentials, gcp_project_id = google.auth.default()
    # instantiate a pubsub subscriber client and subscriber class
    subscriber_client = pubsub.SubscriberClient(credentials=credentials)
    subscriber = Subscriber(subscriber_client, gcsfs)

    # use current time for filenames
    current_time = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    # subscribe to impressions topic to retrieve messages and write to bucket
    print('*** processing impressions ***')
    logger.info('*** processing impressions ***')

    impressions_bucket = os.getenv('IMPRESSIONS_BUCKET')
    subscription_path = subscriber_client.subscription_path(gcp_project_id, 'news_impressions')
    impressions_filename = 'impression-{}'.format(current_time)
    impressions_messages = subscriber.get_messages(subscription_path, impressions_bucket, impressions_filename)

    # subscribe to clicks topic to retrieve messages and write to bucket
    print('*** processing clicks ***')
    logger.info('*** processing clicks ***')

    clicks_bucket = os.getenv('CLICKS_BUCKET')
    subscription_path = subscriber_client.subscription_path(gcp_project_id, 'news_clicks')
    clicks_filename = 'clicks-{}'.format(current_time)
    clicks_messages = subscriber.get_messages(subscription_path, clicks_bucket, clicks_filename)

    # return impressions_messages
    return 'Pulled tracking messages from topic', 200


def load_tracking():
    """This function will load tracking files in the storage bucket to the BigQuery tables 
    """
    credentials, gcp_project_id = google.auth.default()
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

    return 'Loaded tracking data to BigQuery', 200

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

    credentials, gcp_project_id = google.auth.default()
    bigquery_client = bigquery.Client(project=gcp_project_id, credentials=credentials)
    gcs_client = storage.Client(project=gcp_project_id, credentials=credentials)

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
    loader = Loader(bigquery_client, gcs_client, gcsfs_client)
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
        dataset_id = 'news'
        articles_table_id = 'articles'   
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
    get_tracking()

    retries = 3
    count = 1
    status = None
    while (status != 200 or count < retries):
        time.sleep(10)
        print('loading tracking (attempt: {}'.format(count))
        logger.info('loading tracking (attempt: {}'.format(count))
        status = load_tracking()[1]
        print('loading tracking status {}'.format(status))
        logger.info('loading tracking status {}'.format(status))

        if status == 200:
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