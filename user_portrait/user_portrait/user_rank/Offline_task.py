#-*- coding:utf-8 -*-
import datetime
import time
from elasticsearch import Elasticsearch
from time_utils import ts2datetime, datetime2ts,ts2date
from global_utils import es_user_portrait as es


#es = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)

USER_RANK_KEYWORD_TASK_INDEX = 'user_rank_keyword_task'
USER_RANK_KEYWORD_TASK_TYPE = 'offline_task'

MAX_ITEMS = 2 ** 10

def add_task( user_name  , key ,type = "keyword",range = "all" ):
    start_time = time.time()
    
    body_json = {
                'submit_user' : user_name ,
                'keyword' : key,
                'start_time' : str(ts2date(start_time)) ,
                'type' : type,
                'status':0,
                'range' : range , 
                'user_ts' : user_name +  str(start_time)
            }
    try:
        print "add a item to user_rand_keyword_task"
        es.index(index = USER_RANK_KEYWORD_TASK_INDEX , doc_type=USER_RANK_KEYWORD_TASK_TYPE ,  body=body_json)
        return body_json["user_ts"]
    except Exception , e1 :
        print e1

def search_user_task(user_name):
    c_result = {}
    query = {"query":{"bool":{"must":[{"term":{"offline_task.submit_user":user_name}}],"must_not":[],"should":[]}},"from":0,"size":MAX_ITEMS,"sort":[],"facets":{}}
    try:
        result = es.search(index=USER_RANK_KEYWORD_TASK_INDEX , doc_type=USER_RANK_KEYWORD_TASK_TYPE , body=query)['hits']
        c_result['flag'] = True
        c_result['data'] = result
        return c_result
    except Exception , e1 :
        c_result['flag'] = False
        c_result['data'] = e1
        print e1
        return c_result
    

if __name__ == "__main__":
    add_task(user_name = "kanon" , key = "hello" , type = "keyword" )

    
    

        
            
