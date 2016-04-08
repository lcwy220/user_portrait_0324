#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys
import datetime
import time as TIME
from INDEX_TABLE import *
 
from time_utils import ts2datetime, datetime2ts
from global_utils import es_user_portrait as es

#MAX_SIZE = 2 ** 10
MAX_SIZE = 100

def all_sort_filter( uid_list = [] , sort_norm = 'imp' , time = 1 , key_search = False ):
    uid = []
    if sort_norm == 'bci':
        uid = history_sort('bci_',BCIHIS_INDEX_NAME,BCIHIS_INDEX_TYPE,uid_list,time,False , key_search)
    elif sort_norm == 'bci_change':
        uid = history_sort('bci_',BCIHIS_INDEX_NAME,BCIHIS_INDEX_TYPE,uid_list,time,True ,  key_search)
    elif sort_norm == 'ses':
        uid = history_sort('sensitive_',SESHIS_INDEX_NAME,SESHIS_INDEX_TYPE,uid_list,time,False, key_search)
    elif sort_norm == 'ses_change':
        uid = history_sort('sensitive_',SESHIS_INDEX_NAME,SESHIS_INDEX_TYPE,uid_list,time,True, key_search)
    elif sort_norm == 'fans':
        uid = es_get_userlist_by_all("fansnum" , uid_list, key_search)
    elif sort_norm == 'weibo_num':
        uid = es_get_userlist_by_all("statusnum" , uid_list, key_search)
    return uid



def history_sort( prefix ,index_name , index_type , uid_list , time , ischange = False ,key_search = False):
    sort_field = prefix
    if time == 1 :
        sort_field += "day_change"
    elif time == 7:
        if ischange:
            sort_field += "week_change"
        else:
            sort_field += "week_ave"
    else:
        if ischange:
            sort_field += "month_change"
        else:
            sort_field += "month_ave"

    query = {}
    if key_search:
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
                "sort": { sort_field : { "order": "desc" } },
                "size" : MAX_SIZE
            }
    else :
        query = {
            "sort": { sort_field : { "order": "desc" } },
            "size" : MAX_SIZE 
        }
    try:
        result = es.search(index = index_name , doc_type = index_type , body = query)['hits']['hits']
        uid_list = []
        for item in result :
            uid_list.append(item['_id'].encode("utf-8") )
        return uid_list
    except Exception,e:
        print e
        raise  Exception(index_name  + " " + index_type + " " + str(query).replace("\'","\""))    



def es_get_userlist_by_all(fieldname , uid, key_search = False):
    sort = { fieldname : { "order": "desc" } }
    query = {}
    if key_search:
        query = {
            "query": {
                "filtered": {
                    "filter": {
                        "terms": {
                            "uid": uid
                            }
                        }
                    }
                },
                "sort": sort,
                "fields": [ "uid" ],
                "size" : MAX_SIZE
        }    
    else :
        print "aa"
        query = {
            "query": {
                "bool": {
                    "must": [],
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

    try:
        print str(query).replace("\'","\"")
        result = es.search(index = WEIBO_USER_INDEX_NAME , doc_type = WEIBO_USER_INDEX_TYPE , body = query)['hits']['hits']
        uid_list = []
        for item in result :
            uid_list.append(item['_id'].encode("utf-8") )
        return uid_list
    except Exception,e:
        print e
        raise  Exception('user_list failed!')  
