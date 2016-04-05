#-*- coding:utf-8 -*-

import sys
import time
import json
import math
from user_portrait.global_utils import R_RECOMMENTATION as r
from user_portrait.parameter import DAY, WEEK, RUN_TYPE, RUN_TEST_TIME, MAX_VALUE
from user_portrait.time_utils import ts2datetime, datetime2ts, ts2date
from user_portrait.global_utils import es_user_profile, portrait_index_name, portrait_index_type
from user_portrait.global_utils import ES_CLUSTER_FLOW1 as es_cluster
from user_portrait.global_utils import es_group_result, group_index_name, group_index_type,\
        es_social_sensing, sensing_index_name, sensing_doc_type,\
        es_sentiment_task, sentiment_keywords_index_name, sentiment_keywords_index_type,\
        es_network_task, network_keywords_index_name, network_keywords_index_type

def get_user_operation(submit_user):
    result = {}
    #step1: get user recommentation in operation
    result_recommentation = get_recommentation(submit_user)
    #result.append(result_recommentation)
    result['recomment'] = result_recommentation # result_recommentation = []
    #step2: get user group detect task
    result_group_detect = get_group_detect(submit_user)
    result['group_detect'] = result_group_detect # result_group_detect = []
    #step3: get user group analysis task
    result_group_analysis = get_group_analysis(submit_user)
    result['group_analysis'] = result_group_analysis # result_group_analysis = []
    #step4: get user sentiment trend task
    result_sentiment_task = get_sentiment_task(submit_user)
    result['sentiment_task'] = result_sentiment_task # result_sentiment_task = []
    #step5: get user user rank task
    #step6: get user network task
    #step7: get user social sensing task
    result_sensing_task = get_sensing_task(submit_user)
    result['sensing_task'] = result_sensing_task # result_sensing_task = []
    return result


# use to get group detect task from es--group_manage_v2
def get_group_detect(submit_user):
    results = []
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term': {'submit_user': submit_user}},
                            {'term': {'task_type': 'detect'}}
                            ]
                        }
                    }
                }
            },
        'sort':[{'submit_date': {'order': 'desc'}}],
        'size': MAX_VALUE
        }
    #search group task
    try:
        group_task_result = es_group_result.search(index=group_index_name, doc_type=group_index_type,\
                body=query_body)['hits']['hits']
    except:
        group_task_result = []
    #group task results
    for group_item in group_task_result:
        source = group_item['_source']
        task_name = source['task_name']
        task_process = source['detect_process']
        submit_ts = source['submit_date']
        submit_date = ts2date(submit_ts)
        state = source['state']
        task_type = source['detect_type']
        results.append([task_name, submit_date, state, task_type, task_process])
    return results

#use to get group analysis task
def get_group_analysis(submit_user):
    results = []
    #step1: get query body
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term': {'submit_user': submit_user}},
                            {'term': {'task_type': 'analysis'}}
                            ]
                        }
                    }
                }
            },
        'sort': [{'submit_date': {'order': 'desc'}}],
        'size': MAX_VALUE
        }
    #step2: search
    try:
        group_task_result = es_group_result.search(index=group_index_name, doc_type=group_index_type,\
                body=query_body)['hits']['hits']
    except:
        group_task_result = []
    #step3: task results
    for group_item in group_task_result:
        source = group_item['_source']
        task_name = source['task_name']
        if not task_name:
            continue
        task_status = source['status']
        submit_ts = source['submit_date']
        submit_date = ts2date(submit_ts)
        try:
            state = source['state']
        except:
            state = ''
        results.append([task_name, submit_date, state, task_status])

    return results

def get_sentiment_task(submit_user):
    results = []
    #test
    submit_user = 'admin@qq.com'
    #step1:query_body
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                     'term': {'submit_user': submit_user}
                    }
                }
            },
        'size': MAX_VALUE
        }
    #step2:search
    try:
        sentiment_task_result = es_sentiment_task.search(index=sentiment_keywords_index_name,\
                doc_type=sentiment_keywords_index_type, body=query_body)['hits']['hits']
    except:
        sentiment_task_result = []
    #step3:query results
    for task_item in sentiment_task_result:
        source = task_item['_source']
        task_id = source['task_id']
        query_keywords = source['query_keywords']
        submit_ts = source['submit_ts']
        submit_date = ts2date(submit_ts)
        start_date = source['start_date']
        end_date = source['end_date']
        status = source['status']
        results.append([task_id, query_keywords, start_date, end_date, \
                submit_date, status, submit_ts])
    #step4:sort by query_ts
    sort_results = sorted(results, key=lambda x:x[6],reverse=True)
    return sort_results

def get_sensing_task(submit_user):
    results = []
    return results


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

  
