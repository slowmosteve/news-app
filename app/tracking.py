import uuid
from flask import session, request, make_response, after_this_request, redirect

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

def track_and_get_url(article_id, articles, user_id):
    """Tracks a link click based on article ID and returns the article URL

    Args:
        article_id: article ID which can be mapped to article metadata
        articles: list of dictionaries with article metadata
        user_id: ID of the user
    """

    # find the dictionary for the article clicked in the list of article dictionaries
    article_dict = next(item for item in articles if item['article_id'] == article_id)
    article_title = article_dict['title']
    article_url = article_dict['url']
    print("""
    *********************
    tracking link click for article id: {}
    user id: {}
    article_title: {}
    article_url: {}
    *********************
    """.format(article_id, user_id, article_title, article_url, user_id)
    )
    return article_url