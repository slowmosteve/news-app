import os
import gcsfs
from flask import Flask, request
from subscriber import Subscriber
from google.cloud import pubsub

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
    # instantiate a pubsub subscriber client
    subscriber_client = pubsub.SubscriberClient.from_service_account_json('./.creds/news-site-subscriber.json')
    subscription_path = subscriber_client.subscription_path(GCP_PROJECT_ID, 'news_impressions')

    subscriber = Subscriber(subscriber_client, gcsfs)
    messages = subscriber.get_messages(subscription_path)

    return messages

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8081

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    # app.run(host='127.0.0.1', port=PORT, debug=True)
    app.run(host='0.0.0.0', port=PORT, debug=True)