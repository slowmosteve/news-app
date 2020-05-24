import requests
import os
from flask import Flask, request, render_template, session
from collections import defaultdict

app = Flask(__name__)

app.secret_key = os.getenv('FLASK_SESSION_SECRET')
app.config['SESSION_TYPE'] = 'filesystem'

@app.route('/', methods=['GET'])
def index():
    return ('Server running', 200)

@app.route('/home')
def home():
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

    articles = []

    # loop through results and add them to the message to be published
    for i in range(publish_range):
        # create empty list of article details
        details = defaultdict(list)

        # populate fields from news results
        result = response.json()['articles'][i]
        columns = ['title','author','description','content','url','urlToImage','publishedAt']
        for column in columns:
            details[column] = result[column]
        articles.append(details)
    
    print('articles: '+str(articles))

    return render_template('home.html', title='Home', articles=articles)

@app.route('/about')
def about():
    return render_template("about.html")

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)