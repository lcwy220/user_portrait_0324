#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys
import datetime
from time_utils import ts2datetime, datetime2ts
from parameter import DAY, LOW_INFLUENCE_THRESHOULD
from Sort_norm_filter import sort_norm_filter
from Make_up_user_info import make_up_user_info

USER_INDEX_NAME = 'user_portrait_1222'
USER_INDEX_TYPE = 'user'

es_9200 = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)
es_9206 = Elasticsearch(['219.224.134.213:9206', '219.224.134.214:9206'], timeout = 6000)

USER_RANK_KEYWORD_TASK_INDEX = 'user_rank_keyword_task'
USER_RANK_KEYWORD_TASK_TYPE = 'offline_task'


MAX_ITEMS = 2**28

def key_words_search(username ,  pre , during , start_time , keyword , type  = 'in' , search_key = '' , sort_norm = ''  ,time = 1):
    keywords = keyword.split(",")
    should = []
    for key in keywords:
        should.append(   {"prefix":{"text.text":key}}  )    
    date = start_time 
    index_name = pre + start_time
    while not es_9206.indices.exists(index= index_name) :
        during = datetime2ts(date) + DAY
        date = ts2datetime(during)
        index_name = pre + date
        during -= 1

    
    uid_set = set()
    for i in range(during):
        print index_name
        query = {"query":{"bool":{"must":[],"must_not":[],"should":should}},"size":MAX_ITEMS,"sort":[],"facets":{},"fields":['uid']}
        try :
            temp = es_9206.search(index = index_name , doc_type = 'text' , body = query)
            result = temp['hits']['hits']
            print "Fetch " + str(len(result))
            for item in result :
                uid_set.add(item['fields']['uid'][0].encode("utf-8") )
        except Exception,e:
            print e
            raise  Exception('user_list failed!')        
        during = datetime2ts(date) + DAY
        date = ts2datetime(during)
        index_name = pre + date
        i += 1
    result_list = []
    if type == 'in' :
        query = {"fields":[],"size":MAX_ITEMS}
        in_set = set()
        try :
            result = es_9200.search(index = USER_INDEX_NAME , doc_type = USER_INDEX_TYPE , body = query)['hits']['hits']
            
            for item in result :
                in_set.add(item['_id'].encode("utf-8") )

            result_list = list(uid_set & in_set)
        except Exception,e:
            print e
            raise  Exception('user_list failed!')      

    else :
        result_list = list(uid_set)
    if type == 'in':
        uid_list = sort_norm_filter(result_list,sort_norm ,time,False)
    else:
        uid_list = sort_norm_filter(result_list,sort_norm ,time,True)
    results = make_up_user_info(uid_list)

    query = {"query":{"bool":{"must":[{"term":{"offline_task.user_ts":search_key}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}

    try:
        result = es_9200.search(index = USER_RANK_KEYWORD_TASK_INDEX , doc_type = USER_RANK_KEYWORD_TASK_TYPE , body = query)['hits']['hits']
        item = result[0]
        item['_source']['status'] = 1
        item['_source']['results'] = results
        es_9200.index(index = USER_RANK_KEYWORD_TASK_INDEX , doc_type=USER_RANK_KEYWORD_TASK_TYPE , id=item['_id'],  body=item)
    except Exception,e:
        print e
        raise  Exception('user_list failed!')  
    return results

    
    
    


    
            
    