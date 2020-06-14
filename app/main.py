import requests
import os
import uuid
from flask import Flask, request, render_template, session, make_response, after_this_request, redirect
from tracking import check_or_set_user_id, count_hits, track_click_and_get_url, track_impressions
from google.cloud import pubsub

app = Flask(__name__)

app.secret_key = os.getenv('FLASK_SESSION_SECRET')
app.config['SESSION_TYPE'] = 'filesystem'
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

@app.route('/', methods=['GET'])
def index():
    return ('Server running', 200)

@app.route('/home')
def home():
    # set GCp project and instantiate pubsub client

    print("GCP project: {}".format(GCP_PROJECT_ID))
    pubsub_client = pubsub.PublisherClient.from_service_account_json('./.creds/news-site-publisher.json')

    # check the user ID or set a new one on the cookie
    user_id = check_or_set_user_id()

    # count hits for the current user ID
    user_hits = count_hits()

    api_key = os.getenv('NEWS_API_KEY')
    url_base = 'https://newsapi.org'
    url_path = '/v2/top-headlines'
    url_params = {
        # 'country': 'ca',
        'language': 'en',
        'apiKey': api_key
        }
    print('requesting news for endpoint: {}, params: {}'.format(url_path, url_params))

    url_params['apiKey'] = api_key
    request_url = str(url_base + url_path)
    response = requests.get(request_url, params=url_params)

    print('status: '+str(response.json()['status']))

    # set the publish range based on the number of results
    max_results = 10
    if (response.json()['totalResults'] > max_results):
        publish_range = max_results
    else:
        publish_range = response.json()['totalResults']

    global articles
    articles = []

    # loop through results and add them to the message to be published
    for i in range(publish_range):
        # create empty list of article details
        details = {}

        # populate fields from news results
        result = response.json()['articles'][i]
        columns = ['title','author','description','content','url','urlToImage','publishedAt']
        details['article_id'] = str(uuid.uuid4())
        for column in columns:
            details[column] = result[column]
        articles.append(details)

    track_impressions(GCP_PROJECT_ID, pubsub_client, articles, user_id)

    resp = make_response(render_template('home.html', title='Home', articles=articles, user_hits=user_hits, user_id=user_id))
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