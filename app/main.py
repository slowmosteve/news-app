import requests
import os
import uuid
from flask import Flask, request, render_template, session, make_response, after_this_request, redirect
from collections import defaultdict

app = Flask(__name__)

app.secret_key = os.getenv('FLASK_SESSION_SECRET')
app.config['SESSION_TYPE'] = 'filesystem'

def check_or_set_user_id():
    """checks if the user_id exists on the cookie otherwise generates a new one and sets it on the cookie

    Returns:
        resp: a Flask request response object 
    """
    # check if user_id exists on cookie otherwise generate a new one (not encrypted on cookie)
    user_id = request.cookies.get('user_id')
    if user_id:
        print("Found user_id: {}".format(user_id))
        resp = make_response()
        session['user_id'] = user_id
        return user_id
    else:
        user_uuid = uuid.uuid4()
        print("Generating new ID: {}".format(user_uuid))
        encoded_user_uuid = str(user_uuid).encode('utf-8')
        resp = make_response(redirect('/home'))
        resp.set_cookie('user_id', encoded_user_uuid)

         # use deferred callback to set a cookie with the user ID
        @after_this_request
        def remember_user_id(response):
            resp.set_cookie('user_id', encoded_user_uuid)
            session['user_id'] = encoded_user_uuid
            return resp

def count_hits():
    """Returns the total hits for the current user stored on the Flask session
    
    Returns:
        hits: The number of hits for the current user ID 
    """
    hits = session.get('hits', None)
    if not hits:
        session['hits'] = 1
    else:
        session['hits']+=1
        print("The Total Number of refreshes for this user is: "+str(session['hits']))

    return hits


@app.route('/', methods=['GET'])
def index():
    return ('Server running', 200)

@app.route('/home')
def home():
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
        details = defaultdict(list)

        # populate fields from news results
        result = response.json()['articles'][i]
        columns = ['title','author','description','content','url','urlToImage','publishedAt']
        details['article_id'] = str(uuid.uuid4())
        for column in columns:
            details[column] = result[column]
        articles.append(details)

    resp = make_response(render_template('home.html', title='Home', articles=articles, user_hits=user_hits, user_id=user_id))
    return resp

@app.route('/static/tracking/<article_id>')
def tracking_article_view(article_id):
    """Tracks the article clicked prior to redirecting the user
    """
    # get the user_id from cookie
    user_id = check_or_set_user_id()

    # find the dictionary for the article clicked in the list of article dictionaries
    article_dict = next(item for item in articles if item['article_id'] == article_id)
    article_title = article_dict['title']
    article_url = article_dict['url']
    print("""
    *********************
    tracking link click for article id: {}
    user id: {}
    article_title: {}
    article_url: {}
    *********************
    """.format(article_id, user_id, article_title, article_url)
    )
    return redirect(article_url)

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