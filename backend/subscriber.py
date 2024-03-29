import json
import logging

logger = logging.getLogger('app.subscriber')

class Subscriber:
    def __init__(self, subscriber_client, gcsfs_client):
        """Instantiates the Subscriber class for accessing messages from a Pubsub subscription
        and writing messages to Cloud Storage
        Args:
            subscriber_client: Pubsub subscriber client
            gcsfs_client: a client for GCSFS
        """
        self.subscriber_client = subscriber_client
        self.gcsfs_client = gcsfs_client

    def write_messages_to_file(self, message_list, bucket_path, filename):
        """Write pubsub messages to NDJSON file to be loaded to BigQuery
        Args:
            message_list: list of dictionaries to be converted to NDJSON file
            bucket_path: GCS bucket path e.g. "gs://bucket_path"
            filename: name of the resulting file without extension
        """
        
        with self.gcsfs_client.open('{}/{}.ndjson'.format(bucket_path, filename), 'w') as f:
            for item in message_list:
                f.write(item+'\n')

        return 'Messages written to file', 200

    def get_messages(self, subscription_path, bucket_name, filename):
        """Retrieves messages from a Pubsub topic and writes to bucket

        Args:
            subscription_path: a Pubsub subscription path
            bucket_name: Cloud Storage bucket name
            filename: name of the resulting file without extension
        """
        try:
            # maximum messages to process
            max_messages = 10

            response = self.subscriber_client.pull(
                request={
                    'subscription': subscription_path, 
                    'max_messages': max_messages
                }
            )

            ack_ids = []
            message_list = []
            for received_message in response.received_messages:
                print('Received message ID: {} | published {}'.format(received_message.message.message_id, received_message.message.publish_time))
                logger.info('Received message ID: {} | published {}'.format(received_message.message.message_id, received_message.message.publish_time))
                ack_ids.append(received_message.ack_id)
                decoded_message = json.loads(received_message.message.data.decode('utf-8'))
                message_list.append(decoded_message)

            bucket_path = 'gs://{}'.format(bucket_name)
            
            # only write files with messages
            if message_list:
                self.write_messages_to_file(message_list, bucket_path, filename)
                print('wrote file {} to bucket {}'.format(filename, bucket_path))
                logger.info('wrote file {} to bucket {}'.format(filename, bucket_path))
            else:
                print('no messages found')
                logger.info('no messages found')

            # Acknowledges the received messages so they will not be sent again.
            self.subscriber_client.acknowledge(
                request={
                    'subscription': subscription_path, 
                    'ack_ids': ack_ids
                }
            )

            return 'Received and acknowledged {} messages'.format(len(response.received_messages)), 200

        except Exception as e:
            print(e)
            logger.info(e)

            return 'Failed to get messages', 400

