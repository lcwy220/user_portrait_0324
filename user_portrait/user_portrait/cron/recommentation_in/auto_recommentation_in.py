# -*- coding: UTF-8 -*-
import sys
import json
import time

reload(sys)
sys.path.append('../../')
from global_utils import R_RECOMENTATION, es_tag, tag_index_name, tag_index_type,\
        es_user_portrait, portrait_index_name, portrait_index_type, \
        es_group_result, group_index_name, group_index_type
from parameter import DAY,RUN_TYPE, RUN_TEST_TIME, RECOMMEND_IN_AUTO_DATE,\
        RECOMMEND_IN_AUTO_SIZE, RECOMMEND_IN_AUTO_GROUP
from time_utils import datetime2ts, ts2datetime

# get recommentation from hotspot information
def get_hotspot_recommentation():
    results = []
    return results


# get admin user
def get_admin_user():
    user_list = []
    return user_list

# get recomment history
def get_recomment_history(admin_user, now_date):
    results = set()
    now_ts = datetime2ts(now_date)
    for i in range(RECOMMEND_IN_AUTO_DATE, 0, -1):
        iter_date = ts2datetime(now_ts - i * DAY)
        submit_user_recomment = 'recoment_' + admin_user + '_' + str(date)
        recomment_user_list = set(r.hkeys(submit_user_recomment))
        results = results | recomment_user_list

    return results

# get tag history
def get_tag_history(admin_user, now_date):
    results = set()
    now_ts = datetime2ts(now_date)
    search_tag_list = []
    query_date_list = []
    for i in range(RECOMMEND_IN_AUTO_DATE, 0, -1):
        iter_date = ts2datetime(now_ts - i * DAY)
        query_date_list.append(iter_date)
    attribute_query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'terms': {'date': query_date_list}},
                            {'term': {'user': admin_user}}
                            ]
                        }
                    }
                }
            }
        }
    try:
        attribute_result = es_tag.get(index=attribute_index_name, doc_type=attribute_index_type,\
                body=attribute_query_body)['hits']['hits']
    except:
        attribute_result = []
    tag_query_list = []
    for attribute_item in attribute_result:
        attribute_item_source = attribute_item['_source']
        attribute_name = attribute_item_source['attribute_name']
        attribute_value_string = attribute_item_source['attribute_value']
        item_tag_list = [attribute_name + '-' + attribute_value for attribute_value in attribute_value_string]
        tag_query_list.extend(item_tag_list)
    submit_user_attribute = admin_user + '-tag'
    portrait_query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'terms': {submit_user_attribute: tag_query_list}
                    }
                }
            },
        'size': RECOMMEND_IN_AUTO_SIZE
        }
    try:
        portrait_result = es_user_portrait.search(index=portrait_index_name, doc_type=portrait_index_type,\
                body=portrait_query_body, _source=False)['hits']['hits']
    except:
        portrait_result = []
    results = set([item['_id'] for item in portrait_result])

    return results

# get group history
def get_group_history(admin_user, now_date):
    results = set()
    now_ts = datetime2ts(now_date)
    start_ts = now_ts - DAY * RECOMMEND_IN_AUTO_DATE
    end_ts = now_ts
    #search group task
    query_body = {
        'query':{
            'bool':{
                'must':[
                    {'range': {'submit_date':{'gte': start_ts, 'lt': end_ts}}},
                    {'term': {'submit_user': admin_user}},
                    {'term': {'task_type': 'analysis'}}
                    ]
                }
            },
        'size': RECOMMEND_IN_AUTO_GROUP
        }
    try:
        group_results = es_group_result.search(index=group_index_name, doc_type=group_index_type,\
                body=query_body, _source=False, fields=['uid_list'])['hits']['hits']
    except:
        group_results = []
    all_user_list = []
    for group_item in group_results:
        uid_list = group_item['fields'][0]['uid_list']
        all_user_list.extend(uid_list)
    results = set(all_user_list)
    return results

# get social sensing history
def get_sensing_history(admin_user, now_date):
    results = set()
    
    return results

# get extend by history record
def get_extend(all_set):
    extend_result = set()
    return extend_result

# get recommentation from admin user operation
def get_operation_recommentation():
    results = {}
    now_ts = time.time()
    now_date = ts2datetime(now_ts)
    admin_user_list = get_admin_user()
    for admin_user in admin_user_list:
        #step1: recommentation record
        recommentation_history_result = get_recomment_history(admin_user, now_date)
        #step2: add tag record
        tag_history_result = get_tag_history(admin_user, now_date)
        all_set = recommentation_history_result | tag_history_result
        #step3: group analysis record
        group_history_result = get_group_history(admin_user, now_date)
        all_set = all_set | group_history_result
        #step4: social sensing record
        sensing_result = get_sensing_history(admin_user, now_date)
        all_set = all_set | sensing_result
        #step5: extend by all set
        extend_result = get_extend(all_set)
        results[admin_user] = extend_result

    return results

# save results
def save_results(save_type, recomment_results):
    save_mark = False
    if save_type == 'hotspot':
        print 'save hotspot results'
    elif save_type == 'operation':
        print 'save operation results'
    return save_mark

# get user auto recommentation
def compute_auto_recommentation():
    #step1: get recommentation from hotspot information
    hotspot_results = get_hotspot_recommentation()
    save_type = 'hotspot'
    save_results(save_type, hotspot_results)
    #step2: get recommentation from admin user operation
    operation_results = get_operation_recommentation()
    save_type = 'operation'
    save_results(save_type, operation_results)




if __name__=='__main__':
    log_time_start_ts = time.time()
    log_time_start_date = ts2datetime(log_time_start_ts)
    print 'cron/recommend_in/recommend_in_auto.py&start&' + log_time_start_date

    compute_auto_recommentaion()

    log_time_end_ts = time.time()
    log_time_end_date = ts2datetime(log_time_end_ts)
    print 'cron/recommend_in/recommend_in_auto.py&end&' + log_time_end_date
