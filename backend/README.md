# backend

This directory defines the backend that stores data from the Pubsub queue that the app publishes to.

The backend uses a scheduled pull subscription to retrieve messages from the queue, batch the messages together and write them to a storage bucket before loading the file to the BigQuery table.