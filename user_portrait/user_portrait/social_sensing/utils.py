# -*- coding:utf-8 -*-

import sys
import json
import math
import time
from get_task_detail import get_task_detail_2
from full_text_serach import get_origin_weibo_detail, get_retweet_weibo_detail, get_positive_weibo_detail
from user_portrait.global_utils import es_user_profile as es_profile
from user_portrait.global_utils import es_user_portrait as es
from user_portrait.global_utils import es_flow_text as es_text
from user_portrait.global_utils import profile_index_name, profile_index_type, portrait_index_name, portrait_index_type, \
                         flow_text_index_name_pre, flow_text_index_type
from user_portrait.parameter import INDEX_MANAGE_SOCIAL_SENSING as index_manage_sensing_task
from user_portrait.parameter import DOC_TYPE_MANAGE_SOCIAL_SENSING as task_doc_type
from user_portrait.parameter import DETAIL_SOCIAL_SENSING as index_sensing_task

def get_warning_detail(task_name, ts, user):
    results = dict()
    index_name = task_name # 可能的index-name

    results = get_task_detail_2(task_name, ts, user)


    return results

# 获得一段时间内的文本，按序排列
def get_text_detail(task_name, ts, text_type, user, order, size=100):
    results = []
    _id = user + '-' + task_name
    task_detail = es.get(index=index_manage_sensing_task, doc_type=task_doc_type, id=_id)["_source"]
    social_sensors = json.loads(task_detail["social_sensors"])

    #print social_sensors
    if int(text_type) == 0: # 热门原创微博
        results = get_origin_weibo_detail(ts, user, task_name, size, order, 1)

    elif int(text_type) == 1: # 热门转发微博
        results = get_origin_weibo_detail(ts, user, task_name, size, order, 2)

    elif int(text_type) == 2: # 普通转发微博
        results = get_retweet_weibo_detail(ts, user, task_name, size, "message_type", 3)

    elif int(text_type) == 3: # 普通评论微博
        results = get_retweet_weibo_detail(ts, user, task_name, size, "message_type", 2)

    elif int(text_type) == 4: # 积极微博
        results = get_retweet_weibo_detail(ts, user, task_name, size, "sentiment", "1")

    elif int(text_type) == 5: # 中性微博
        results = get_retweet_weibo_detail(ts, user, task_name, size, "sentiment", "0")

    elif int(text_type) == 6: # 消极微博
        results = get_retweet_weibo_detail(ts, user, task_name, size, "sentiment", ["2", "3", "4", "5", "6"])

    else:
        print "error"

    return results





