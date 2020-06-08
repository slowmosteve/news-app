# news-site

This repository defines a news website built on the Flask framework using data from NewsApi.org

## Setup

`bin/setup.sh` is used to complete the following steps:
- configure Python version and packages using `pyenv` and `pipenv`
- prompt for the API key which will be stored in `.env` and picked up by `pipenv` as an environment variable
- create a `FLASK_SESSION_SECRET` using `uuidgen` and store in `.env`

Note that `.env` should be excluded from git.

From the root directory run: 

```
source bin/setup.sh
```

## Running the app locally

Use `pipenv` to run Python commands in a virtual environment with requirements installed through the `Pipfile`. For example, we can run the Flask app locally using:

```
pipenv run python app/main.py
```