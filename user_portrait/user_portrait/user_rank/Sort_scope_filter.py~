from elasticsearch import Elasticsearch
import sys

es = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)

USER_INDEX_NAME = "user_portrait"
USER_INDEX_TYPE = "user"

MAX_ITEMS = 300

def sort_scope_filter(sort_scope = 'in_nolimit' , arg = None):
    uid = []
    if sort_scope == 'in_nolimit':
        uid = user_portrait_filter()
    elif sort_scope == 'in_limit_domain':
        uid = user_portrait_filter('doamin',arg)
    elif sort_scope == 'in_limit_topic':
        uid = user_portrait_filter('topic_string',arg)
    elif sort_scope == 'in_limit_geo':
        uid = user_portrait_filter('activity_geo',arg)
    elif sort_scope == 'in_limit_hashtag':
        uid = user_portrait_filter('hashtag',arg)
    elif sort_scope == 'in_limit_keyword':
        pass
    elif sort_scope == 'all_nolimit':
        pass
    elif sort_scope == 'all_limit_keyword':
        pass

    return uid

def user_portrait_filter(prefix_key = None, prefix_value =None ):
    query = ''
    if prefix_key == None :
        query = {"query":{"bool":{"must":[],"must_not":[],"should":[]}},"size":MAX_ITEMS,"fields":[]}
    else :
        query = {"query":{"bool":{"must":[],"must_not":[],"should":[{"prefix":{prefix_key:prefix_value}}]}},"size":MAX_ITEMS,"facets":{},"fields":[]}
    try:
        result =  es.search(index = USER_INDEX_NAME , doc_type = USER_INDEX_TYPE , body = query)['hits']['hits']
        uid_list = []
        for item in result :
            uid_list.append(item['_id'].encode("utf-8"))
        return uid_list
    except Exception,e:
        print e
        raise  Exception('get user_portrait_filter user list failed!')




    
    
    
