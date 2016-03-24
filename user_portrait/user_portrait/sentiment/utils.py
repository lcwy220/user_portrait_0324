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
from user_portrait.time_utils import ts2datetime


#use to get all sentiment trend by date
def search_sentiment_all(start_date, end_date):
    results = {}

    return results

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
def search_sentiment_domain(domain, start_date, end_date):
    results = {}
    return results

#use to get topic sentiment trend by date for user in user_portrait
def search_senitment_topic(topic, start_date, end_date):
    results = {}
    return results

#use to get sentiment trend point weibo and keywords and user
def search_sentiment_weibo_keywords(start_ts, task_type, time_segment):
    results = {}
    #step1: identify the task type
    #step2: get weibo
    #step3: get keywords
    #step4: get user who in user_portrait or not

    return results
