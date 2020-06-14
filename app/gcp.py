import json
import base64
from google.cloud import bigquery, pubsub, storage

def pubsub_publish(topic_name, message_data):
    """Function that publishes a message to a GCP Pub/Sub topic
    Args:
        topic_name: Pub/Sub topic name
        message_data: JSON message to be published
    """
    pubsub_client = pubsub.PublisherClient()
    json_data = json.dumps(message_data)
    data_payload = base64.urlsafe_b64encode(bytearray(json_data, 'utf8'))
    print_log("Publishing message: {}".format(json_data))
    message_future = pubsub_client.publish(topic_name, data=data_payload)
    message_future.add_done_callback(pubsub_callback)

def pubsub_callback(message_future):
    """Return a callback with errors or an update ID upon publishing messages to Pub/Sub
    Args:
        topic_name: Pub/Sub topic name
        message_future: Pub/Sub message
    """
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print_log("Failed to publish message. exception: {}.".format(message_future.exception()))
    else:
        print_log("Published message update id: {}".format(message_future.result()))
