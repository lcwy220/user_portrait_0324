# -*-coding:utf-8-*-
import time
import sys
import json
import numpy as np
from myconfig import * 
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_portrait as es_9200
from global_utils import flow_text_index_name_pre, flow_text_index_type
from global_utils import redis_flow_text_mid as r_flow
from parameter import DAY, RUN_TYPE
from time_utils import ts2datetime, datetime2ts



def mapper_bci_today(todaydate=None):
    if todaydate:
        BCI_INDEX_NAME = BCI_INDEX_NAME_PRE + ts2datetime((datetime2ts(todaydate) - DAY)).replace("-","")
        TODAY_TIME = todaydate
    else :
        BCI_INDEX_NAME = BCI_INDEX_NAME_PRE + '20130901'
        TODAY_TIME = '2013-09-02'
    s_re = scan(es_9200, query={"query":{"match_all":{}},"size":MAX_ITEMS ,"fields":[TOTAL_NUM,TODAY_BCI]}, index=BCI_INDEX_NAME, doc_type=BCI_INDEX_TYPE)
    count = 0
    array = []
    while 1:
        try:
            temp = s_re.next()
            one_item = {}
            one_item['id'] = temp['_id'].encode("utf-8")
            one_item['total_num'] = temp['fields'][TOTAL_NUM][0]
            one_item['today_bci'] = temp['fields'][TODAY_BCI][0]
            one_item['update_time'] = TODAY_TIME
            array.append(one_item)
            count += 1
            if count % 1000 == 0:
                r_flow.lpush('update_bci_list', json.dumps(array))
                array = []
                count = 0
        except StopIteration: 
                print "all done" 
                r_flow.lpush('update_bci_list', json.dumps(array))              
                break 

    #s_re.close()

def mapper_bci_history(todaydate=None):
    if todaydate:
        TODAY_TIME = todaydate
    es_query = {"query":{"bool":{"must":[],"must_not":[{"term":{"bci.update_time":TODAY_TIME}}],"should":[{"match_all":{}}]}},"from":0,"size":MAX_ITEMS,"fields":[]}

    s_re = scan(es_9200, query=es_query, index=BCIHIS_INDEX_NAME, doc_type=BCIHIS_INDEX_TYPE)
    count = 0
    array = []
    while 1:
        try:
            temp = s_re.next()
            one_item = {}
            one_item['id'] = temp['_id'].encode("utf-8")
            one_item['total_num'] = 0
            one_item['today_bci'] = 0
            one_item['update_time'] = TODAY_TIME
            array.append(one_item)
            count += 1
            if count % 1000 == 0:
                r_flow.lpush('update_bci_list', json.dumps(array))
                array = []
                count = 0
        except StopIteration: 
                print "all done" 
                r_flow.lpush('update_bci_list', json.dumps(array))
                break 



if __name__ == '__main__':
    todaydate = ts2datetime(time.time())
    mapper_bci_today(todaydate)
    mapper_bci_history(todaydate)
