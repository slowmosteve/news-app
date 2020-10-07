# news-site

This repository defines a news website built in Google Cloud Platform using the Flask framework with data from NewsApi.org

![news site diagram](./news-site.png "News Site Diagram")

## Setup

`bin/setup.sh` is used to complete the following steps:
- configure Python version and packages using `pyenv` and `pipenv`
- prompt for the API key which will be stored in `.env` and picked up by `pipenv` as an environment variable
- create a `FLASK_SESSION_SECRET` using `uuidgen` and store in `.env`
- prompt for the `GCP_PROJECT_ID` and store in `.env`

Note that `.env` should be excluded from git.

From the root directory run: 

```
source bin/setup.sh
```

## GCP Credentials

This project uses Google Cloud for services like Cloud Run, Pub/Sub, Cloud Storage and BigQuery. Service accounts keys are stored in the `/.creds` directory which is excluded from git.

## GCP Setup

Resources needed for this project:
- Storage bucket
- Pubsub topics `news_impressions` and `news_clicks`
- Pubsub pull subscriptions `news_impressions` and `news_clicks`
- BigQuery dataset `news` with tables for `impressions` and `clicks`
- Publisher service account (Pubsub publisher)
- Subscriber service account (Pubsub viewer, Pubsub publisher, Storage admin)
- Loader service account (Storage admin, BQ data editor)

## Running the app locally

Use `pipenv` to run Python commands in a virtual environment with requirements installed through the `Pipfile`. For example, we can run the Flask app locally using:

```
pipenv run python app/main.py
```

## Running the app as a container

Use `docker` to build an image from the `/app` directory.

```
docker build . -t app
```

Run the app with port forwarding using the following. Note that the service account key is mounted as a volume and a file with environment variables is passed to the container.

```
docker run --rm -p 127.0.0.1:8080:8080 -v ~/projects/news-site/.creds/news-site.json:/creds/news-site.json --env-file ../.docker_env/docker_env app
```

Similaryl, use `docker` to build an image from the `/backend` directory. Note that at the time of writing `papermill` has some incompatibilities that can be worked around by using `pipenv lock --pre` 

```
docker build . -t backend
```

Run the backend with the following:

```
docker run --rm -p 127.0.0.1:8081:8081 -v ~/projects/news-site/.creds/news-site.json:/creds/news-site.json --env-file ../.docker_env/docker_env backend
```

## Running on Cloud Run

From the `/app` directory, build the app container and publish on Container Registry `gcloud builds submit --tag gcr.io/[project id]/news_app`

Deploy app `gcloud run deploy news-app --image gcr.io/[project id]/news_app`

From the `/backend` directory, build the app container and publish on Container Registry `gcloud builds submit --tag gcr.io/[project id]/backend`

Deploy app `gcloud run deploy backend --image gcr.io/[project id]/backend`

Set env vars using the `--update-env-vars` flag:

```
gcloud run deploy backend --image gcr.io/[project-id]/backend --update-env-vars ENV=prod,GCP_PROJECT_ID=[project-id],SECRETS_BUCKET=secrets-[project-id],ARTICLES_BUCKET=articles-[project-id],ARTICLES_PROCESSED_BUCKET=articles-processed-[project-id],IMPRESSIONS_BUCKET=impressions-[project-id],IMPRESSIONS_PROCESSED_BUCKET=impressions-processed-[project-id],CLICKS_BUCKET=clicks-[project-id],CLICKS_PROCESSED_BUCKET=clicks-processed-[project-id],NOTEBOOK_BUCKET=notebook-[project-id]
```