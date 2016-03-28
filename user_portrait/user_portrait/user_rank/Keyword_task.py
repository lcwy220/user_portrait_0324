#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys
import json
import datetime
from time_utils import ts2datetime, datetime2ts
from parameter import DAY, LOW_INFLUENCE_THRESHOULD
from in_filter import in_sort_filter
from all_filter import all_sort_filter
from Makeup_info import make_up_user_info

USER_INDEX_NAME = 'user_portrait_1222'
USER_INDEX_TYPE = 'user'

es_9200 = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)
es_9206 = Elasticsearch(['219.224.134.213:9206', '219.224.134.214:9206'], timeout = 6000)

USER_RANK_KEYWORD_TASK_INDEX = 'user_rank_keyword_task'
USER_RANK_KEYWORD_TASK_TYPE = 'user_rank_task'


MAX_ITEMS = 2**28

def key_words_search( search_type , pre , during , start_time , keyword , search_key = '' , sort_norm = '', sort_scope = ''  ,time = 1 , isall = False):
    keywords = keyword.split(",")
    should = []
    for key in keywords:
        if search_type == "hashtag":
            should.append({"prefix":{"text.text": "#" +  key + "#"}})
        else:    
            should.append({"prefix":{"text.text":key}})    
    date = start_time 
    index_name = pre + start_time
    while not es_9206.indices.exists(index= index_name) :
        new_time = datetime2ts(date) + DAY
        date = ts2datetime(new_time)
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
        new_time = datetime2ts(date) + DAY
        date = ts2datetime(new_time)
        index_name = pre + date
        i += 1
    result_list = []
    if not isall :
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
    if not isall:
        uid_list = in_sort_filter(time,sort_norm ,sort_scope ,None , result_list , True)
    else:
        uid_list = all_sort_filter(result_list , sort_norm , time ,True)
    results = make_up_user_info(uid_list,isall,time,sort_norm)

    query = {"query":{"bool":{"must":[{"term":{"user_rank_task.user_ts":search_key}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}

    if True:
        result = es_9200.search(index = USER_RANK_KEYWORD_TASK_INDEX , doc_type = USER_RANK_KEYWORD_TASK_TYPE , body = query)['hits']['hits']
        search_id = result[0]['_id']
        item = result[0]['_source']
        item['status'] = 1
        item['result'] = json.dumps(results)
        es_9200.index(index = USER_RANK_KEYWORD_TASK_INDEX , doc_type=USER_RANK_KEYWORD_TASK_TYPE , id=search_id,  body=item)
    return results
   
    
def scan_offlice_task():
    
    query = {"query":{"bool":{"must":[{"term":{"user_rank_task.status":"0"}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}
    results = es_9200.search(index = USER_RANK_KEYWORD_TASK_INDEX , doc_type = USER_RANK_KEYWORD_TASK_TYPE,body=query)['hits']
    if results['total'] > 0 :
        for item in results['hits']:
            search_type = item['_source']['search_type']          
            pre = item['_source']['pre']
            during =  item['_source']['during'] 
            start_time =  item['_source']['start_time']  
            keyword = item['_source']['keyword'] 
            search_key = item['_source']['user_ts']
            sort_norm = item['_source']['sort_norm']
            sort_scope = item['_source']['sort_scope']
            time = item['_source']['time']
            isall = item['_source']['isall']
            key_words_search( search_type , pre , during , start_time , keyword , search_key , sort_norm , sort_scope  ,time , isall)
    

if __name__ == "__main__":
    scan_offlice_task();
    

    
            
    