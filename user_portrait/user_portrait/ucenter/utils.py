#-*- coding:utf-8 -*-

import sys
import time
import json
import math
from user_portrait.global_utils import R_RECOMMENTATION as r
from user_portrait.parameter import DAY, WEEK, RUN_TYPE, RUN_TEST_TIME
from user_portrait.time_utils import ts2datetime, datetime2ts
from user_portrait.global_utils import es_user_profile, portrait_index_name, portrait_index_type
from user_portrait.global_utils import ES_CLUSTER_FLOW1 as es_cluster

def get_user_operation(submit_user):
    result = []
    #step1: get user recommentation in operation
    result_recommentation = get_recommentation(submit_user)
    result.append(result_recommentation)
    #step2: get user group detect task 
    #step3: get user group analysis task
    #step4: get user sentiment trend task
    #step5: get user user rank task
    return result



def get_recommentation(submit_user):
    if RUN_TYPE:
        now_ts = time.time()
    else:
        now_ts = datetime2ts(RUN_TEST_TIME)

    in_portrait_set = set(r.hkeys("compute"))
    result = []
    for i in range(7):
        iter_ts = now_ts - i*DAY
        iter_date = ts2datetime(iter_ts)
        submit_user_recomment = "recomment_" + submit_user + "_" + str(iter_date)
        bci_date = ts2datetime(iter_ts - DAY)
        submit_user_recomment = r.hkeys(submit_user_recomment)
        bci_index_name = "bci_" + bci_date.replace('-', '')
        exist_bool = es_cluster.indices.exists(index=bci_index_name)
        if not exist_bool:
            continue
        if submit_user_recomment:
            user_bci_result = es_cluster.mget(index=bci_index_name, doc_type="bci", body={'ids':submit_user_recomment}, _source=True)['docs']
            user_profile_result = es_user_profile.mget(index='weibo_user', doc_type='user', body={'ids':submit_user_recomment}, _source=True)['docs']
            max_evaluate_influ = get_evaluate_max(bci_index_name)
            for i in range(len(submit_user_recomment)):
                uid = submit_user_recomment[i]
                bci_dict = user_bci_result[i]
                profile_dict = user_profile_result[i]
                try:
                    bci_source = bci_dict['_source']
                except:
                    bci_source = None
                if bci_source:
                    influence = bci_source['user_index']
                    influence = math.log(influence/max_evaluate_influ['user_index'] * 9 + 1 ,10)
                    influence = influence * 100
                else:
                    influence = ''
                try:
                    profile_source = profile_dict['_source']
                except:
                    profile_source = None
                if profile_source:
                    uname = profile_source['nick_name']
                    location = profile_source['user_location']
                    fansnum = profile_source['fansnum']
                    statusnum = profile_source['statusnum']
                else:
                    uname = ''
                    location = ''
                    fansnum = ''
                    statusnum = ''
                if uid in in_portrait_set:
                    in_portrait = "1"
                else:
                    in_portrait = "0"
                recomment_day = iter_date
                result.append([iter_date, uid, uname, location, fansnum, statusnum, influence, in_portrait])

    return result    
   

def get_evaluate_max(index_name):
    max_result = {}
    index_type = 'bci'
    evaluate_index = ['user_index']
    for evaluate in evaluate_index:
        query_body = {
            'query':{
                'match_all':{}
                },
            'size':1,
            'sort':[{evaluate: {'order': 'desc'}}]
            }
        try:
            result = es_cluster.search(index=index_name, doc_type=index_type, body=query_body)['hits']['hits']
        except Exception, e:
            raise e
        max_evaluate = result[0]['_source'][evaluate]
        max_result[evaluate] = max_evaluate
    return max_result

  
