import os
import logging

logger = logging.getLogger('app.articles')

class Articles:
    def __init__(self, bigquery_client):
        """Instantiates the Articles class for retrieving articles from BigQuery
        Args:
            bigquery_client: pass existing BigQuery client as an argument
        """
        self.bigquery_client = bigquery_client

    def get_articles(self, user_id):
        """
        Args:
            user_id: User ID from the browser cookie
        Returns:
            articles: List of dictionaries containing article data
        """
        latest_articles_table = os.getenv('ARTICLES_TABLE')
        personalized_articles_table = os.getenv('PERSONALIZED_ARTICLES_TABLE')
        articles_query = """
            WITH latest_articles AS (
                SELECT
                    * EXCEPT (load_timestamp, article_order)
                FROM `{}`
            ),
            personalized_articles AS (
                SELECT
                    'personalized' AS sort,
                    * EXCEPT (user_id, topic, total_clicks, user_already_clicked, article_order, load_timestamp)
                FROM `{}`
                WHERE
                    user_id = '{}'
                    AND user_already_clicked = FALSE
                ORDER BY
                    total_clicks DESC, publishedAt DESC
                LIMIT 10
            )
            SELECT * FROM latest_articles
            UNION ALL
            SELECT * FROM personalized_articles
        """.format(latest_articles_table, personalized_articles_table, user_id)
        
        print('running query: {}'.format(articles_query))
        logger.info('running query: {}'.format(articles_query))

        # run query and store results in list of dictionaries
        query_job = self.bigquery_client.query(articles_query)
        articles = []
        for row in query_job:
            articles.append(dict(row))

        # convert datetime objects to strings
        for index, article in enumerate(articles):
            article['publishedAt'] = article['publishedAt'].strftime('%Y-%m-%d %H:%M:%S')

        return articles

    