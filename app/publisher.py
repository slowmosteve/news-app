import json
import base64
from google.cloud import pubsub

def pubsub_publish(project_id, publisher_client, topic_name, message_data):
    """Function that publishes a message to a GCP Pub/Sub topic
    Args:
        project_id: GCP project ID
        publisher_client: Pubsub publisher client
        topic_name: Pub/Sub topic name
        message_data: JSON message to be published
    """
    json_data = json.dumps(message_data)
    data_payload = base64.urlsafe_b64encode(bytearray(json_data, 'utf8'))
    print("Publishing message: {}".format(json_data))

    topic_path = publisher_client.topic_path(project_id, topic_name)
    print("Topic path: {}".format(topic_path))
    message_future = publisher_client.publish(topic_path, data=data_payload)
    message_future.add_done_callback(pubsub_callback)

def pubsub_callback(message_future):
    """Return a callback with errors or an update ID upon publishing messages to Pub/Sub
    Args:
        topic_name: Pub/Sub topic name
        message_future: Pub/Sub message
    """
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print("Failed to publish message. exception: {}.".format(message_future.exception()))
    else:
        print("Published message update id: {}".format(message_future.result()))
