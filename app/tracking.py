import uuid
import json
import datetime
from flask import session, request, make_response, after_this_request, redirect
from publisher import pubsub_publish, pubsub_callback

def check_or_set_user_id():
    """checks if the user_id exists on the cookie otherwise generates a new one and sets it on the cookie

    Returns:
        resp: a Flask request response object 
    """
    # check if user_id exists on cookie otherwise generate a new one (not encrypted on cookie)
    user_id = request.cookies.get('user_id')
    if user_id:
        print("Found user_id: {}".format(user_id))
        resp = make_response()
        session['user_id'] = user_id
        return user_id
    else:
        user_uuid = uuid.uuid4()
        print("Generating new ID: {}".format(user_uuid))
        encoded_user_uuid = str(user_uuid).encode('utf-8')
        resp = make_response(redirect('/home'))
        resp.set_cookie('user_id', encoded_user_uuid)

         # use deferred callback to set a cookie with the user ID
        @after_this_request
        def remember_user_id(response):
            resp.set_cookie('user_id', encoded_user_uuid)
            session['user_id'] = encoded_user_uuid
            return resp

def count_hits():
    """Returns the total hits for the current user stored on the Flask session
    
    Returns:
        hits: The number of hits for the current user ID 
    """
    hits = session.get('hits', None)
    if not hits:
        session['hits'] = 1
    else:
        print("Hits from this user: "+str(session['hits']))
        session['hits']+=1

    return hits

def track_impressions(project_id, pubsub_client, articles, user_id):
    """Tracks articles that were presented to a given user regardless of whether they were clicked

    Args:
        project_id: GCP project ID
        pubsub_client: Pubsub publisher client
        articles: list of dictionaries with article metadata
        user_id: ID of the user
    """
    impression_tracker = {}
    impression_tracker['user_id'] = user_id
    impression_tracker['impression_timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    impression_tracker['articles'] = []
    for item in articles:
        # print(item['title'])
        impression_tracker['articles'].append(item)

    print("""
    *********************
    impression tracker: {}
    *********************
    """.format(json.dumps(impression_tracker))
    )

    # publish message to pubsub topic
    pubsub_publish(project_id, pubsub_client, 'news_impressions', json.dumps(impression_tracker))

    return 204

def track_click_and_get_url(project_id, pubsub_client, article_id, articles, user_id):
    """Tracks a link click based on article ID and returns the article URL

    Args:
        project_id: GCP project ID
        pubsub_client: Pubsub publisher client
        article_id: article ID which can be mapped to article metadata
        articles: list of dictionaries with article metadata
        user_id: ID of the user
    """

    # find the dictionary for the article clicked in the list of article dictionaries
    article_dict = next(item for item in articles if item['article_id'] == article_id)
    article_url = article_dict['url'] 

    # populate click tracker dictionary
    click_tracker = {}
    click_tracker['user_id'] = user_id
    click_tracker['click_timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    click_tracker['article_clicked'] = article_dict

    print("""
    *********************
    click tracker: {}
    *********************
    """.format(json.dumps(click_tracker))
    )

    # publish message to pubsub topic
    pubsub_publish(project_id, pubsub_client, 'news_clicks', json.dumps(click_tracker))

    return article_url