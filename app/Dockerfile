# use lightweight slim-buster distribution of python
FROM python:3.7-slim-buster

# Copy local code to the container image.
ENV APP_HOME ./
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN pip install pipenv
RUN pipenv lock --requirements > requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8080

# Run the web service on container startup. 
# Use gunicorn webserver with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app
CMD exec gunicorn --bind :8080 --workers 1 --threads 8 main:app