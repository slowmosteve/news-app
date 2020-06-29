#!/usr/bin sh
echo "Setting up Pyenv and Pipenv"
pyenv local 3.8.0;
pipenv install --skip-lock \
  requests \
  flask \
  google-cloud-storage \
  google-cloud-bigquery \
  google-cloud-pubsub;
read -p "API key for NewsAPI.org: " NEWS_API_KEY;
echo "NEWS_API_KEY=$NEWS_API_KEY" > .env;
export FLASK_SESSION_SECRET=$(uuidgen);
echo "FLASK_SESSION_SECRET=$FLASK_SESSION_SECRET" >> .env;
read -p "GCP Project ID: " GCP_PROJECT_ID;
echo "GCP_PROJECT_ID=$GCP_PROJECT_ID" >> .env;
read -p "Articles bucket name: " ARTICLES_BUCKET;
echo "ARTICLES_BUCKET=$ARTICLES_BUCKET" >> .env;
read -p "Articles processed bucket name: " ARTICLES_PROCESSED_BUCKET;
echo "ARTICLES_PROCESSED_BUCKET=$ARTICLES_PROCESSED_BUCKET" >> .env;
read -p "Impressions bucket name: " IMPRESSIONS_BUCKET;
echo "IMPRESSIONS_BUCKET=$IMPRESSIONS_BUCKET" >> .env;
read -p "Impressions processed bucket name: " IMPRESSIONS_PROCESSED_BUCKET;
echo "IMPRESSIONS_PROCESSED_BUCKET=$IMPRESSIONS_PROCESSED_BUCKET" >> .env;
read -p "Clicks bucket name: " CLICKS_BUCKET;
echo "CLICKS_BUCKET=$CLICKS_BUCKET" >> .env;
read -p "Clicks processed bucket name: " CLICKS_PROCESSED_BUCKET;
echo "CLICKS_PROCESSED_BUCKET=$CLICKS_PROCESSED_BUCKET" >> .env;