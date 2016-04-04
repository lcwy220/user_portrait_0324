# -*- coding:utf-8 -*-
import sys
import json
import time
from user_portrait.global_utils import es_network_task, network_keywords_index_name, \
                                network_keywords_index_type
from user_portrait.global_utils import R_NETWORK_KEYWORDS, r_network_keywords_name

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
