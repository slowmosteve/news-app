import os
from flask import Flask, request
from subscriber import get_messages, callback
from google.cloud import pubsub

app = Flask(__name__)

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

@app.route('/', methods=['GET'])
def index():
    return ('Backend server running', 200)

@app.route('/get_messages', methods=['GET'])
def fetch_data():
    """This route will retrieve messages from the Pubsub topic
    """
    # instantiate a pubsub subscriber client
    subscriber = pubsub.SubscriberClient.from_service_account_json('./.creds/news-site-subscriber.json')
    subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, 'news_impressions')

    messages = get_messages(subscriber, subscription_path)

    return messages


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8081

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)