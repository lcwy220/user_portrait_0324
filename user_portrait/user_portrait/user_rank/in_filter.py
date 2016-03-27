#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys
import datetime
import time as TIME
from INDEX_TABLE import *
from time_utils import ts2datetime, datetime2ts
from global_utils import es_user_portrait as es


MAX_SIZE = 300

def in_sort_filter( time = 1 , sort_norm = 'bci' , sort_scope = 'in_nolimit' , arg = None , uid = []):
    ischange = False
    scope = None
    norm = None
    sort_field = None
    time_field = None
    pre = None
    index = None
    type = None

    if sort_scope == 'in_nolimit':
        pass ;
    elif sort_scope == 'in_limit_domain':
        scope = 'domain';
    elif sort_scope == 'in_limit_topic':
        scope = 'topic_string';
    elif sort_scope == 'in_limt_keyword':
        pass;   #deal it outer 
    elif sort_scope == 'in_limit_hashtag':
        scope = 'hashtag'; #deal it inner
    elif sort_scope == 'in_limit_geo':
        scope = 'activity_geo'
    if sort_norm == 'bci':
        pre = 'bci_'
        ischange = False
        index = BCI_INDEX_NAME
        type = BCI_INDEX_TYPE
    elif sort_norm == 'bci_change':
        pre = 'bci_'
        ischange = True
        index = BCI_INDEX_NAME
        type = BCI_INDEX_TYPE 
    elif sort_norm == 'imp':
        pre = 'importance_'
        ischange = False
        index = IMP_INDEX_NAME
        type = IMP_INDEX_TYPE
    elif sort_norm == 'imp_change':
        pre = 'importance_'
        ischange = True
        index = IMP_INDEX_NAME
        type = IMP_INDEX_TYPE
    elif sort_norm == 'act':
        pre = 'activeness_'
        ischange = False
        index = ACT_INDEX_NAME
        type = ACT_INDEX_TYPE
    elif sort_norm == 'act_change':
        pre = 'activeness_'
        ischange = True
        index = ACT_INDEX_NAME
        type = ACT_INDEX_TYPE 
    elif sort_norm == 'ses':
        pre = 'sensitive_'
        ischange = False
        index = SES_INDEX_NAME
        type = SES_INDEX_TYPE
    elif sort_norm == 'ses_change':
        pre = 'sensitive_'
        ischange = True
        index = SES_INDEX_NAME
        type = SES_INDEX_TYPE 
    
    return es_search(pre ,scope ,arg,index,type,time,ischange , uid)
        
 
def es_search( pre , scope , arg , index_name , type_name  , time , ischange = False , uid_list = []):
    today = TIME.time()
    
    sort = [] 
    sort_field = []
    if time == 1:
            sort_field = pre + 'day_' + 'change'
    elif time == 7 :
        if ischange :
            sort_field = pre + 'week_' + 'change'
        else :
            sort_field = pre + 'week_' + 'ave'
    elif time == 30 :
        if ischange :
            sort_field = pre + 'month_' + 'change'
        else :
            sort_field = pre + 'month_' + 'ave'

    must = []
    if arg :
        must = [{"prefix": {scope: arg }} ]

    if sort_field:
        sort = [{ sort_field : { "order": "desc" } }]
        
    if not uid_list:
        query = {
            "query": {
                "bool": {
                    "must": must,
                    "must_not": [],
                    "should": []
                }
            },
            "sort": sort , 
            "facets": {},
            "fields": [
                "uid"
            ],
            "size" : MAX_SIZE
        }
    else :
        query = {
            "query": {
                 "filtered": {
                    "filter": {
                        "terms": {
                            "uid": uid_list
                            }
                       }       
                  }           
                },
            "sort": [{ sort_field : { "order": "desc" } }],
            "fields" : [],
            "size" : MAX_SIZE
        }        
    try:
        print index_name
        print type_name
        print str(query).replace("\'","\"")
        result = es.search(index = index_name , doc_type = type_name , body = query)['hits']['hits']
        uid_list = []
        for item in result :
            uid_list.append(item['_id'].encode("utf-8") )
        return list(set(uid_list))
    except Exception,e:
        print e
        raise  Exception(index_name + " " + type_name + " " + str(query).replace("\'","\""))


