import json
import base64
import gcsfs
from google.cloud import pubsub

class Subscriber:
    def __init__(self, subscriber_client, gcsfs):
        """Instantiates the Subscriber class for accessing messages from a Pubsub subscription
        and writing messages to Cloud Storage
        Args:
            subscriber_client: Pubsub subscriber client
            gcsfs: a client for GCSFS
        """
        self.subscriber_client = subscriber_client
        self.gcsfs = gcsfs

    def get_messages(self, subscription_path, bucket_name):
        """Retrieves messages from a Pubsub topic and writes to bucket

        Args:
            subscription_path: a Pubsub subscription path
            bucket_name: Cloud Storage bucket name
        """
        try:
            # maximum messages to process
            max_messages = 10

            response = self.subscriber_client.pull(subscription_path, max_messages=max_messages)

            ack_ids = []
            message_list = []
            for received_message in response.received_messages:
                print("Received message ID: {} | published {}".format(received_message.message.message_id, received_message.message.publish_time))
                ack_ids.append(received_message.ack_id)
                decoded_message = json.loads(received_message.message.data.decode('utf-8'))
                print("Message: {}".format(decoded_message))
                message_list.append(decoded_message)

            bucket_path = 'gs://{}'.format(bucket_name)
            self.write_messages_to_file(message_list, bucket_path, "test")

            # Acknowledges the received messages so they will not be sent again.
            self.subscriber_client.acknowledge(subscription_path, ack_ids)

            return "Received and acknowledged {} messages".format(len(response.received_messages)), 200

        except Exception as e:
            print(e)

            return "Failed to get messages", 400

    def write_messages_to_file(self, message_list, bucket_path, filename):
        """Write pubsub messages to NDJSON file to be loaded to BigQuery
        Args:
            message_list: list of dictionaries to be converted to NDJSON file
            bucket_path: GCS bucket path e.g. "gs://bucket_path"
            filename: name of the resulting file without extension
        """
        
        with self.gcsfs.open("{}/{}.ndjson".format(bucket_path, filename), 'w') as f:
            for item in message_list:
                f.write(item+'\n')

        return "Messages written to file", 200
