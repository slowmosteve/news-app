import os
from flask import Flask, render_template, make_response, redirect
import logging
from tracking import check_or_set_user_id, count_hits, track_click_and_get_url, track_impressions
from articles import Articles
from google.cloud import pubsub, bigquery
import google.auth

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

app.secret_key = os.getenv('FLASK_SESSION_SECRET')
app.config['SESSION_TYPE'] = 'filesystem'

@app.route('/', methods=['GET'])
def index():
    return ('Server running', 200)

@app.route('/home')
def home():
    logger = logging.getLogger('app.home')

    # set GCP project and instantiate pubsub client
    credentials, gcp_project_id = google.auth.default()
    pubsub_client = pubsub.PublisherClient(credentials=credentials)
    bigquery_client = bigquery.Client(project=gcp_project_id, credentials=credentials)

    # check the user ID or set a new one on the cookie
    user_id = check_or_set_user_id()

    # count hits for the current user ID
    user_hits = count_hits()
    
    # instantiate global articles for tracking and retrieve list of article dictionaries
    global articles
    articles_client = Articles(bigquery_client)
    articles = articles_client.get_articles(user_id)

    # filter articles based on sort field
    latest_articles = [a for a in articles if a['sort'] == 'latest']
    popular_articles = [a for a in articles if a['sort'] == 'popular']
    random_articles = [a for a in articles if a['sort'] == 'random']
    personalized_articles = [a for a in articles if a['sort'] == 'personalized']

    # use popular articles if personalized articles is empty
    personalized_articles = popular_articles if not personalized_articles else personalized_articles
    
    # track article impressions
    track_impressions(gcp_project_id, pubsub_client, articles, user_id)

    # create flask response
    resp = make_response(
            render_template(
                'home.html',
                title='Home',
                articles=latest_articles,
                articles_v2=personalized_articles,
                articles_random=random_articles,
                user_hits=user_hits,
                user_id=user_id
            )
        )

    return resp

@app.route('/static/tracking/<article_id>')
def tracking_article_view(article_id):
    # instantiate pubsub client
    credentials, gcp_project_id = google.auth.default()
    pubsub_client = pubsub.PublisherClient(credentials=credentials)
    
    # tracks the article clicked prior to redirecting the user
    user_id = check_or_set_user_id()
    redirect_url = track_click_and_get_url(gcp_project_id, pubsub_client, article_id, articles, user_id)

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