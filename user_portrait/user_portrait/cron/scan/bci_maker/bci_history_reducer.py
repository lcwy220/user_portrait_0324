# -*-coding:utf-8-*-
# es user profile ======10.128.55.81
import time
import sys
import redis
import json
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from bci_history_calculator import cal_num_for_bci_history
from myconfig import * 
from bci_history_mapping import bci_history_mapping
reload(sys)
sys.path.append('../../')
from global_utils import es_user_profile as es_9200
from global_utils import flow_text_index_name_pre, flow_text_index_type
from global_utils import redis_flow_text_mid as r_flow
from parameter import DAY, RUN_TYPE
from time_utils import ts2datetime, datetime2ts

def reducer():
    while 1:
        user_set = r_flow.rpop('update_bci_list')
        list=[]
        if user_set:
            items = json.loads(user_set)
            for item in items:
                uid = item['id']
                total_num = item['total_num']
                today_bci = item['today_bci']
                update_time = item['update_time']
                list.append({'index' : {'_index' : BCIHIS_INDEX_NAME , '_type' : BCIHIS_INDEX_TYPE , '_id' : uid }})
                list.append(cal_num_for_bci_history(uid,total_num,today_bci,update_time))
        if list:
            es_9200.bulk(body = list, index = BCIHIS_INDEX_NAME , doc_type = BCIHIS_INDEX_TYPE)
 

if __name__ == "__main__":
    reducer()
 
