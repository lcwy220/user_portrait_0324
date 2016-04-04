# -*-coding:utf-8-*-

import sys
import json
import time
"""
reload(sys)
sys.path.append('./../')
from global_utils import R_CLUSTER_FLOW1 as r
from global_utils import es_user_profile, es_user_portrait
from global_utils import profile_index_name, profile_index_type, portrait_index_name, portrait_index_type
from parameter import RUN_TYPE
"""
from user_portrait.global_utils import R_CLUSTER_FLOW1 as r
from user_portrait.global_utils import es_user_profile, es_user_portrait
from user_portrait.global_utils import profile_index_name, profile_index_type, portrait_index_name, portrait_index_type
from user_portrait.parameter import RUN_TYPE

def get_temporal_rank(task_type, sort="retweeted"):
    if int(task_type) == 0: # 到目前位置
        sort_list = r.zrange("influence_%s" %sort, 0, 100, withscores=True, desc=True)
    elif int(task_type) == 1:
        sort_list = r.zrange("influence_%s_1" %sort, 0, 100, withscores=True, desc=True)
    elif int(task_type) == 2:
        sort_list = r.zrange("influence_%s_2" %sort, 0, 100, withscores=True, desc=True)
    elif int(task_type) == 3:
        sort_list = r.zrange("influence_%s_3" %sort, 0, 100, withscores=True, desc=True)
    else:
        sort_list = r.zrange("influence_%s_4" %sort, 0, 100, withscores=True, desc=True)

    uid_list = []
    for item in sort_list:
        uid_list.append(item[0])

    if sort == "retweeted":
        other = "comment"
    else:
        other = "retweeted"

    results = []
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
                tmp.append(item['photo_url'])
                tmp.append(item['user_location'])
                tmp.append(item['fansnum'])
            else:
                tmp.extend([_id,'','','',''])
            count_1 = int(sort_list[index][1])
            if int(task_type) == 0:
                count_2 = int(r.zscore("influence_%s" %other, _id))
            else:
                count_2 = int(r.zscore("influence_%s_%s" %(other,task_type), _id))
            if sort == "retweeted":
                tmp.append(count_1)
                tmp.append(count_2)
            else:
                tmp.append(count_2)
                tmp.append(count_1)
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

if __name__ == "__main__":
    print get_temporal_rank(1, "comment")
