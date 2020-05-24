#!/usr/bin sh
echo "Setting up Pyenv and Pipenv"
pyenv local 3.8.0;
pipenv install --skip-lock \
  requests \
  flask \
  google-cloud-storage;
read -p "API key for NewsAPI.org: " NEWS_API_KEY;
echo "NEWS_API_KEY=$NEWS_API_KEY" > .env;
FLASK_SESSION_SECRET=uuidgen;
echo "FLASK_SESSION_SECRET=$FLASK_SESSION_SECRET" >> .env;