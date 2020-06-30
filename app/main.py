import requests
import os
import uuid
import json
from flask import Flask, request, render_template, session, make_response, after_this_request, redirect
from tracking import check_or_set_user_id, count_hits, track_click_and_get_url, track_impressions
from google.cloud import pubsub, bigquery

app = Flask(__name__)

app.secret_key = os.getenv('FLASK_SESSION_SECRET')
app.config['SESSION_TYPE'] = 'filesystem'
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

@app.route('/', methods=['GET'])
def index():
    return ('Server running', 200)

@app.route('/home')
def home():
    # set GCP project and instantiate pubsub client

    print("GCP project: {}".format(GCP_PROJECT_ID))
    pubsub_client = pubsub.PublisherClient.from_service_account_json('./.creds/news-site-publisher.json')
    bigquery_client = bigquery.Client.from_service_account_json('./.creds/news-site-bq-loader.json')

    # check the user ID or set a new one on the cookie
    user_id = check_or_set_user_id()

    # count hits for the current user ID
    user_hits = count_hits()

    # instantiate global articles for tracking default and random
    global articles
    articles = []

    # get latest articles from bigquery
    latest_articles = os.getenv('ARTICLES_TABLE')
    articles_query = """
        SELECT
            * EXCEPT (load_timestamp, article_order)
        FROM `{}`
    """.format(latest_articles)
    print('running query: {}'.format(articles_query))

    # run query and store results in list of dictionaries
    query_job = bigquery_client.query(articles_query)
    for row in query_job:
        articles.append(dict(row))

    # convert datetime objects to strings for default
    for i in articles:
        for field in i:
            if field == 'publishedAt':
                i['publishedAt'] = i['publishedAt'].strftime('%Y-%m-%d %H:%M:%S')

    # filter articles based on sort field
    latest_articles = [d for d in articles if d['sort'] == 'latest']
    popular_articles = [d for d in articles if d['sort'] == 'popular']
    random_articles = [d for d in articles if d['sort'] == 'random']

    # track article impressions
    track_impressions(GCP_PROJECT_ID, pubsub_client, articles, user_id)

    # create flask response
    resp = make_response(
            render_template(
                'home.html', 
                title='Home', 
                articles=latest_articles, 
                articles_v2=popular_articles, 
                articles_random=random_articles, 
                user_hits=user_hits, 
                user_id=user_id
            )
        )

    return resp

@app.route('/static/tracking/<article_id>')
def tracking_article_view(article_id):
    # instantiate pubsub client
    pubsub_client = pubsub.PublisherClient.from_service_account_json('./.creds/news-site-publisher.json')

    # tracks the article clicked prior to redirecting the user
    user_id = check_or_set_user_id()
    redirect_url = track_click_and_get_url(GCP_PROJECT_ID, pubsub_client, article_id, articles, user_id)

    return redirect(redirect_url)

@app.route('/about')
def about():
    # check the user ID or set a new one on the cookie
    user_id = check_or_set_user_id()

    # count hits for the current user ID
    user_hits = count_hits()

    resp = make_response(render_template('about.html', title='About', user_hits=user_hits, user_id=user_id))
    return resp

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)