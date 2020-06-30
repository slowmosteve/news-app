import json
import base64
from google.cloud import pubsub

class Publisher:
    def __init__(self, publisher_client):
        """Instantiates the Publisher class for publishing messages to PubSub
        Args:
            publisher_client: Pubsub subscriber client
        """
        self.publisher_client = publisher_client

    def pubsub_publish(self, topic_path, message_data):
        """Function that publishes a message to a GCP Pub/Sub topic
        Args:
            topic_name: Pub/Sub topic name
            message_data: JSON message to be published
        """
        json_data = json.dumps(message_data)
        data_payload = json_data.encode('utf-8')
        # print("Publishing message: {}".format(json_data))

        message_future = self.publisher_client.publish(topic_path, data=data_payload)
        message_future.add_done_callback(self.pubsub_callback)

    def pubsub_callback(self, message_future):
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
