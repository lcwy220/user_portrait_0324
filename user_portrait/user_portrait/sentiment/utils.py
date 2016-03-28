#-*- coding:utf-8 -*-
import sys
import json
import time

from user_portrait.global_utils import es_user_portrait, portrait_index_name,\
                                     portrait_index_type, es_flow_text, flow_text_index_name_pre ,\
                                     flow_text_index_type
from user_portrait.global_utils import es_sentiment_task, sentiment_keywords_index_name, \
                                     sentiment_keywords_index_type
from user_portrait.global_utils import R_SENTIMENT_KEYWORDS, r_sentiment_keywords_name
from user_portrait.global_utils import R_DOMAIN_SENTIMENT, r_domain_sentiment_pre,\
        R_TOPIC_SENTIMENT, r_topic_sentiment_pre, R_SENTIMENT_ALL
from user_portrait.time_utils import ts2datetime, datetime2ts
from user_portrait.parameter import DAY, domain_en2ch_dict

sentiment_type_list = ['0', '1', '7']
str2segment = {'fifteen': 900, 'hour': 3600, 'day':3600*24}

def get_new_ts_count_dict(ts_count_result, time_segment, date_item):
    result = {}
    now_date_ts = datetime2ts(date_item)
    segment = str2segment[time_segment]
    for ts in ts_count_result:
        new_ts = int((int(ts) - now_date_ts) / segment) * segment + now_date_ts
        try:
            result[new_ts] += int(ts_count_result[ts])
        except:
            result[new_ts] = int(ts_count_result[ts])
    for ts in range(0, DAY, segment):
        iter_ts = now_date_ts + ts
        if iter_ts not in result:
            result[iter_ts] = 0
    return result

#use to get all sentiment trend by date
def search_sentiment_all(start_date, end_date, time_segment):
    results = {}
    start_ts = datetime2ts(start_date)
    end_ts = datetime2ts(end_date)
    search_date_list = []
    for i in range(start_ts, end_ts + DAY, DAY):
        iter_date = ts2datetime(i)
        search_date_list.append(iter_date)
    sentiment_ts_count_dict = {}
    for sentiment in sentiment_type_list:
        sentiment_ts_count_dict[sentiment] = []
        for date_item in search_date_list:
            iter_r_name = date_item + '_' + sentiment + '_all'
            #get ts_count_dict in one day
            ts_count_result = R_SENTIMENT_ALL.hgetall(iter_r_name)
            #get x and y list by timesegment
            new_ts_count_dict = get_new_ts_count_dict(ts_count_result, time_segment, date_item)
            sort_new_ts_count = sorted(new_ts_count_dict.items(), key=lambda x:x[0])
            sentiment_ts_count_dict[sentiment].extend(sort_new_ts_count)

    return sentiment_ts_count_dict

def union_dict(objs):
    _keys = set(sum([obj.keys() for obj in objs], []))
    _total = {}
    for _key in _keys:
        _total[_key] = sum([int(obj.get(_key, 0)) for obj in objs])
    return _total


#use to get all portrait sentiment trend by date
def search_sentiment_all_portrait(start_date, end_date, time_segment):
    sentiment_ts_count_dict = {}
    start_ts = datetime2ts(start_date)
    end_ts = datetime2ts(end_date)
    search_date_list = []
    domain_list = domain_en2ch_dict.keys()
    for i in range(start_ts, end_ts + DAY, DAY):
        iter_date = ts2datetime(i)
        search_date_list.append(iter_date)
    for sentiment in sentiment_type_list:
        sentiment_ts_count_dict[sentiment] = []
        for date_item in search_date_list:
            ts_count_result_list = []
            for domain in domain_list:
                iter_r_name = r_domain_sentiment_pre + date_item + '_' + sentiment + '_' + domain
                #get ts_count_dict in one day
                ts_count_result = R_DOMAIN_SENTIMENT.hgetall(iter_r_name)
                ts_count_result_list.append(ts_count_result)
            #union all domain to get all portrait
            all_ts_count_result = union_dict(ts_count_result_list)
            #get x and y list by timesegment
            new_ts_count_dict = get_new_ts_count_dict(all_ts_count_result, time_segment, date_item)
            sort_new_ts_count = sorted(new_ts_count_dict.items(), key=lambda x:x[0])
            sentiment_ts_count_dict[sentiment].extend(sort_new_ts_count)
    return sentiment_ts_count_dict
    



#use to get all keywords sentiment trend by date
def search_sentiment_all_keywords(keywords_string, start_date, end_date):
    results = {}
    return results


#use to submit keywords sentiment trend task to date
def submit_sentiment_all_keywords(keywords_string, start_date, end_date, submit_user):
    task_information = {}
    #step1: add task to sentiment_keywords es
    #step2: add task to redis queue
    add_keywords_string = '&'.join(keywords_string.split(','))
    task_information['query_keywords'] = add_keywords_string
    task_information['query_range'] = 'all_keywords'
    task_information['submit_user'] = submit_user
    submit_ts = int(time.time())
    task_information['submit_ts'] = submit_ts
    task_information['start_date'] = start_date
    task_information['end_date'] = end_date
    task_id = submit_ts + '_' + submit_user + '_' + add_keywords_string
    task_information['task_id'] = task_id
    #add to sentiment task information
    try:
        es_sentiment_task.index(index_name=sentiment_keywords_index_name, \
            index_type=sentiment_keywords_index_type, id=task_id, \
            body=task_information)
    except:
        return 'es error'
    #add to sentiment task queue
    try:
        R_SENTIMENT_KEYWORDS.lpush(r_sentiment_keywords_name, json.dumps(task_information))
    except:
        return 'redis error'

    return True

#use to get domain sentiment trend by date for user in user_portrait
def search_sentiment_domain(domain, start_date, end_date, time_segment):
    results = {}
    start_ts = datetime2ts(start_date)
    end_ts = datetime2ts(end_date)
    search_date_list = []
    for i in range(start_ts, end_ts+DAY, DAY):
        iter_date = ts2datetime(i)
        search_date_list.append(iter_date)
    sentiment_ts_count_dict = {}
    for sentiment in sentiment_type_list:
        sentiment_ts_count_dict[sentiment] = []
        for date_item in search_date_list:
            iter_r_name = r_domain_sentiment_pre + date_item + '_' + sentiment + '_' + domain
            #get ts_count_dict in one day
            ts_count_result = R_DOMAIN_SENTIMENT.hgetall(iter_r_name)
            #get x and y list by timesegment
            new_ts_count_dict = get_new_ts_count_dict(ts_count_result, time_segment, date_item)
            sort_new_ts_count = sorted(new_ts_count_dict.items(), key=lambda x:x[0])
            sentiment_ts_count_dict[sentiment].extend(sort_new_ts_count)

    return sentiment_ts_count_dict

#use to get topic sentiment trend by date for user in user_portrait
def search_sentiment_topic(topic, start_date, end_date, time_segment):
    results = {}
    start_ts = datetime2ts(start_date)
    end_ts = datetime2ts(end_date)
    search_date_list = []
    for i in range(start_ts, end_ts+DAY, DAY):
        iter_date = ts2datetime(i)
        search_date_list.append(iter_date)
    sentiment_ts_count_dict = {}
    for sentiment in sentiment_type_list:
        sentiment_ts_count_dict[sentiment] = []
        for date_item in search_date_list:
            iter_r_name = r_topic_sentiment_pre + date_item + '_' + sentiment + '_' + topic
            #get ts_count_dict in one day
            ts_count_result = R_TOPIC_SENTIMENT.hgetall(iter_r_name)
            #get x and y list by timesegment
            new_ts_count_dict = get_new_ts_count_dict(ts_count_result, time_segment, date_item)
            sort_new_ts_count = sorted(new_ts_count_dict.items(), key=lambda x:x[0])
            sentiment_ts_count_dict[sentiment].extend(sort_new_ts_count)

    return sentiment_ts_count_dict


def search_sentiment_detail_all(start_ts, task_type, task_detail, time_segment):
    results = {}
    return results

def search_sentiment_detail_all_keywords(start_ts, task_type, task_detail, time_segment):
    results = {}
    return results

def search_sentiment_detail_in_all(start_ts, task_type, task_detail, time_segment):
    results = {}
    return results

def search_sentiment_detail_in_domain(start_ts, task_type, task_detail, time_segment):
    results = {}
    return results

def search_sentiment_detail_in_topic(start_ts, task_type, task_detail, time_segment):
    results = {}
    return results

#use to get sentiment trend point weibo and keywords and user
def search_sentiment_weibo_keywords(start_ts, task_type, task_detail, time_segment):
    results = {}
    #step1: identify the task type
    #step2: get weibo
    #step3: get keywords
    #step4: get user who in user_portrait or not
    if task_type=='all':
        results = search_sentiment_detail_all(start_ts, task_type, task_detail, time_segment)
    elif task_type=='all-keywords':
        results = search_sentiment_detail_all_keywords(start_ts, task_type, task_detail, time_segment)
    elif task_type=='in-all':
        results = search_sentiment_detail_in_all(start_ts, task_type, task_detail, time_segment)
    elif task_type=='in-domain':
        results = search_sentiment_detail_in_domain(start_ts, task_type, task_detail, time_segment)
    elif task_type=='in-topic':
        results = search_sentiment_detail_in_topic(task_type, task_detail, time_segment)

    return results
