#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys
import datetime
from time_utils import ts2datetime, datetime2ts
from INDEX_TABLE import *

USER_INDEX_NAME = 'user_portrait_1222'
USER_INDEX_TYPE = 'user'

WEBUSER_INDEX_NAME = "weibo_user"
WEBUSER_INDEX_TYPE = "user"


es = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)

def make_up_user_info(user_list = [] , isall = False):
    result_info = []
    print "make up user info's flag : " + str(isall)
    today = str(datetime.date.today())
    today = '2013-09-07'
    timestamp = datetime2ts(today)
    print len(user_list)
    if user_list:
        for id in user_list:
            item = {}
            if isall :
                item = all_makeup_info(id)
            else:
                item = in_makeup_info(id)
            result_info.append(item)
        return result_info
    else:
        return []

def all_makeup_info(id):
    item = {}
    query = {"query":{"bool":{"must":[{"term":{"user.uid":id}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{},"fields":["uid","nick_name","user_location","fansnum"]}
    result = es.search(index=WEBUSER_INDEX_NAME , doc_type=WEBUSER_INDEX_TYPE , body=query)['hits']
    if result['total'] != 0 :
        item['uid'] = result['hits'][0]['fields']['uid'][0]
        item['fans'] = result['hits'][0]['fields']['fansnum'][0]
        item['location'] = result['hits'][0]['fields']['user_location'][0]
        item['uname'] = result['hits'][0]['fields']['nick_name'][0]
    else :
        item['domain'] = None
        item['uid'] = None
        item['topic'] = None
        item['location'] = None
        item['uname'] = None
    item['bci'] = history_info(BCIHIS_INDEX_NAME,BCIHIS_INDEX_TYPE,id,'bci_week_ave')
    item['sen'] = history_info(SESHIS_INDEX_NAME,SESHIS_INDEX_TYPE,id,'sensitive_week_ave')
    return item
        

def in_makeup_info(id):
    item = {}
    query = {"query":{"bool":{"must":[{"term":{"user.uid":id}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{},"fields":["uid","uname","location","topic_string","domain"]}
    result = es.search(index=USER_INDEX_NAME , doc_type=USER_INDEX_TYPE , body=query)['hits']
    if result['total'] != 0 :
        item['domain'] = result['hits'][0]['fields']['domain'][0]
        item['uid'] = result['hits'][0]['fields']['uid'][0]
        item['topic'] = result['hits'][0]['fields']['topic_string'][0]
        item['location'] = result['hits'][0]['fields']['location'][0]
        item['uname'] = result['hits'][0]['fields']['uname'][0]
    else :
        item['domain'] = None
        item['uid'] = None
        item['topic'] = None
        item['location'] = None
        item['uname'] = None
    item['bci'] = history_info(BCI_INDEX_NAME,BCI_INDEX_TYPE,id,'bci_week_ave')
    item['sen'] = history_info(SES_INDEX_NAME,SES_INDEX_TYPE,id,'sensitive_week_ave')
    item['imp'] = history_info(IMP_INDEX_NAME,IMP_INDEX_TYPE,id,'importance_week_ave')
    item['act'] = history_info(ACT_INDEX_NAME,ACT_INDEX_TYPE,id,'activeness_week_ave')
    return item


def history_info(index_name , index_type , uid , fields):
    
    length = len(fields)
    
    query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "uid": uid
                                }
                            }
                        ]
                    }
                },
                "fields": fields
            }
    try:
        result = es.search(index = index_name , doc_type = index_type , body = query)
        if result['timed_out'] == False and result['hits']['total'] != 0 :
            item = result['hits']['hits'][0]['fields']
            return item[fields][0]
        else :
            return None
    except Exception , e:
        print "Exception : " + str(e)
        return None
    
    
