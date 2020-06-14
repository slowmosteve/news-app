import json
import base64
from google.cloud import pubsub
from concurrent.futures import TimeoutError

def callback(message):
    """Acknowledges messages and prints to console
    """
    print("Received message: {}".format(message))
    message.ack()

def get_messages(subscriber_client, subscription_path):
    """Retrieves messages from a Pubsub topic

    Args:
        subscriber_client: a Pubsub subscriber client
        subscription_path: a Pubsub subscription path
    """
    # duration in seconds that the subscriber should listen for messages
    timeout = 10

    streaming_pull_future = subscriber_client.subscribe(subscription_path, callback=callback)
    print("Listening for messages on {}..\n".format(subscription_path))

    with subscriber_client:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
            return 'Completed retrieving messages', 200