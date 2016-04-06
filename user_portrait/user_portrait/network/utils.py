# -*- coding:utf-8 -*-
import sys
import json
import time
from user_portrait.global_utils import es_network_task, network_keywords_index_name, \
                                network_keywords_index_type
from user_portrait.global_utils import es_user_profile, es_user_portrait,\
        es_flow_text, flow_text_index_name_pre, flow_text_index_type
from user_portrait.global_utils import retweet_redis_dict, comment_redis_dict
from user_portrait.global_utils import profile_index_name, profile_index_type, portrait_index_name, portrait_index_type
from user_portrait.global_utils import R_NETWORK_KEYWORDS, r_network_keywords_name
from user_portrait.time_utils import datetime2ts, ts2datetime, ts2date
from user_portrait.parameter import DAY, HOUR, RUN_TYPE
from user_portrait.global_config import R_BEGIN_TIME

begin_ts = datetime2ts(R_BEGIN_TIME)

def show_daily_trend():
    date = ts2datetime(time.time())
    index_name = 'user_portrait_network_count'
    index_type = 'network'
    try:
        results = es_network_task.get(index=index_name, doc_type=index_type, id=date)['_source']
    except:
        results = {}
    return results

def show_daily_rank(period, sort_type, count):
    index_name = 'user_portrait_network'
    index_type = 'network'
    sort = sort_type + '_' + str(period)   #pr_0
    query_body = {
        'query':{},
        'sort':[{sort:{'order': 'desc'}}],
        'size': count
        }
    try:
        seach_results = es_network_task.search(index=index_name, doc_type=index_type, body=query_body)['hits']['hits']
    except:
        search_results = []
    
    results = []
    uid_list = []
    sort_list = []
    for item in search_results:
        source = item['_source']
        uid_list.append(source['uid'])
        sort_list.append(source[sort])
    
    # 查看背景信息
    if uid_list:
        profile_result = es_user_profile.mget(index=profile_index_name, doc_type=profile_index_type, body={"ids":uid_list})["docs"]
        for item in profile_result:
            _id = item['_id']
            index = profile_result.index(item)
            tmp = []
            if item['found']:
                item = item['_source']
                tmp.append(item['uid'])
                tmp.append(item['nick_name'])
                tmp.append(item['statusnum'])
                tmp.append(item['user_location'])
                tmp.append(item['fansnum'])
            else:
                tmp.extend([_id,'','','',''])
            value = int(sort_list[index])
            tmp.append(value)
            results.append(tmp)
    
    if uid_list:
        count = 0
        portrait_result = es_user_portrait.mget(index=portrait_index_name, doc_type=portrait_index_type, body={"ids":uid_list})["docs"]
        for item in portrait_result:
            if item['found']:
                results[count].append("1")
            else:
                results[count].append("0")
            count += 1

    return results

#use to delete network keywords task
def delete_network_keywords(task_id):
    status = False
    es_delete_task.delete(index=network_keywords_index_name, \
                doc_type=network_keywords_index_type, id=task_id)
    status = True
    return status

def submit_network_keywords(keywords_string, start_date, end_date, submit_user):
    task_information = {}
    #step1: add task to network keywords es
    #step2: add task to redis queue
    add_keywords_string = '&'.join(keywords_string.split(','))
    task_information['query_keywords'] = add_keywords_string
    task_information['query_range'] = 'all_keywords'
    task_information['submit_user'] = submit_user
    submit_ts = int(time.time())
    task_information['submit_ts'] = submit_ts
    task_information['start_date'] = start_date
    task_information['end_date'] = end_date
    task_id = str(submit_ts) + '_' + submit_user + '_' + add_keywords_string
    task_information['task_id'] = task_id
    task_information['status'] = '0'
    task_information['results'] = ''
    #add to network task information
    try:
        es_network_task.index(index=network_keywords_index_name, \
            doc_type=network_keywords_index_type, id=task_id, \
            body=task_information)
    except:
        return 'es error'
    #add to network task queue
    try:
        R_NETWORK_KEYWORDS.lpush(r_network_keywords_name, json.dumps(task_information))
    except:
        return 'redis error'

    return True

def search_all_keywords(submit_date, keywords_string, submit_user, start_date, end_date, status):
    results = []
    query_list = []
    if submit_date:
        submit_ts_start = datetime2ts(submit_date)
        submit_ts_end = submit_ts_start + DAY
        query_list.append({'range': {'submit_ts': {'gte': submit_ts_start, 'lt':submit_ts_end}}})
    if keywords_string:
        keywords_list = keywords_string.split(',')
        query_list.append({'terms':{'query_keywords': keywords_list}})
    if submit_user:
        query_list.append({'term': {'submit_user': submit_user}})
    if start_date:
        start_s_ts = datetime2ts(start_date)
        if end_date:
            start_e_ts = datetime2ts(end_date)
        else:
            start_e_ts = start_s_ts + DAY * 30
        start_date_nest_body_list = [ts2datetime(ts) for ts in range(start_s_ts, start_e_ts + DAY, DAY)]
        query_list.append({'terms':{'start_date': start_date_nest_body_list}})
    if end_date:
        end_e_ts = datetime2ts(end_date)
        if start_date:
            end_s_ts = datetime2ts(start_date)
        else:
            end_s_ts = end_e_ts - DAY * 30
        end_date_nest_body_list = [ts2datetime(ts) for ts in range(end_s_ts, end_e_ts + DAY, DAY)]
        query_list.append({'terms': {'end_date': end_date_mest_body_list}})
    if status:
        query_list.append({'term': {'status': status}})
    try:
        task_results = es_network_task.search(index=network_keywords_index_name, \
                doc_type=network_keywords_index_type, body={'query':{'bool':{'must':query_list}}})['hits']['hits']
    except:
        task_results = []
    for task_item in task_results:
        task_source = task_item['_source']
        task_id = task_source['task_id']
        start_date = task_source['start_date']
        end_date = task_source['end_date']
        keywords = task_source['query_keywords']
        submit_ts = ts2date(task_source['submit_ts'])
        status = task_source['status']
        results.append([task_id, start_date, end_date, keywords, submit_ts, status])

    return results

#use to get db_number which is needed to es
def get_db_num(timestamp):
    date = ts2datetime(timestamp)
    date_ts = datetime2ts(date)
    db_number = 2 - (((date_ts - begin_ts) / (DAY * 7))) % 2
    #run_type
    if RUN_TYPE == 0:
        db_number = 1
    return db_number

def search_retweet_network(uid):
    now_ts = time.time()
    now_date_ts = datetime2ts(ts2datetime(now_ts))
    #get redis db number
    db_number = get_db_num(now_date_ts)
    #get redis db
    #retweet_redis = retweet_redis_dict[str(db_number)]
    retweet_redis = comment_redis_dict[str(db_number)]
    item_result = {}
    item_result = retweet_redis.hgetall(item)
    return item_result 

def search_retweet_network_keywords(task_id, uid):
    results = {}
    task_results = es_network_task.get(index=network_keywords_index_name, \
                doc_type=network_keywords_index_type, id=task_id)['_source']

    start_date = task_results['start_date']
    start_ts = datetime2ts(start_date)
    end_date = task_resuts['end_date']
    end_ts = datetime2ts(end_date)
    iter_date_ts = start_ts
    to_date_ts = end_ts
    iter_query_date_list = [] # ['2013-09-01', '2013-09-02']
    while iter_date_ts <= to_date_ts:
        iter_date = ts2datetime(iter_date_ts)
        iter_query_date_list.append(iter_date)
        iter_date_ts += DAY
    #step2: get iter search flow_text_index_name
    #step2.1: get search keywords list
    query_must_list = []
    keyword_nest_body_list = []
    keywords_string = task_results['query_keywords']
    keywords_list = keywords_string.split('&')
    for keywords_item in keywords_list:
        keyword_nest_body_list.append({'wildcard': {'text': '*' + keywords_item + '*'}})
    query_must_list.append({'bool': {'should': keyword_nest_body_list}})
    query_must_list.append({'term': {'uid': uid}})
    #step2.2: iter search by date
    search_results = []
    for iter_date in iter_query_date_list:
        flow_text_index_name = flow_text_index_name_pre + iter_date
        query_body = {
            'query':{
                'bool':{
                    'must':query_must_list
                }
            },
            'size': 100
        }
        flow_text_result = es_flow_text.search(index=flow_text_index_name, doc_type=flow_text_index_type,\
                    body=query_body)['hits']['hits']
        search_results.extend(flow_text_result)
    return search_results 
