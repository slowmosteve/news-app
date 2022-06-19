import logging
import requests
import datetime
import uuid

logger = logging.getLogger('app.news')

class News:
    def __init__(self, api_key):
        """Instantiates the Articles class for retrieving articles from BigQuery
        Args:
            gcs_client: pass existing GCS client as an argument
            api_key: key for newsapi.org 
        """
        self.api_key = api_key

    def get_news(self, date_filter, news_domains):
        """Retrieves news data from newsapi.org and returns the response JSON
        Args:
            date_filter: date range for newsapi filtering
            news_domains: domains to include in results
        Return:
            response.json() from request
        """

        url_base = 'https://newsapi.org'
        url_path = '/v2/everything'
        url_params = {
            'from': date_filter,
            'language': 'en',
            'apiKey': self.api_key,
            'pageSize': 100,
            'sortBy': 'publishedAt',
            'domains': news_domains
            }
        print('requesting news for endpoint: {}, params: {}'.format(url_path, url_params))
        logger.info('requesting news for endpoint: {}, params: {}'.format(url_path, url_params))

        url_params['apiKey'] = self.api_key
        request_url = str(url_base + url_path)
        response = requests.get(request_url, params=url_params)

        print('status: {}'.format(str(response.json()['status'])))
        logger.info('status: {}'.format(str(response.json()['status'])))

        return response.json()

    def format_articles(self, response):
        """Takes response JSON and formats to newline delimited JSON
        Args:
            response: JSON response from newsapi
        Return:
            articles: list of dictionaries with data for each article
        """

        articles = []
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # populate article list with details for each article
        for index, article in enumerate(response['articles']):
            details = {}
            details['article_id'] = str(uuid.uuid4())
            details['article_order'] = index
            details['load_timestamp'] = current_time
            for column in ['title', 'author', 'description', 'content', 'url', 'urlToImage', 'publishedAt']:
                details[column] = article[column]
            articles.append(details)

        print('articles: {}'.format(articles))

        return articles

    