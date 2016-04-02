# -*- coding:utf-8 -*-

import sys
import time
import json

reload(sys)
sys.path.append('../../')
from parameter import DAY, HOUR, RUN_TYPE
from global_config import R_BEGIN_TIME
from global_utils import es_user_portrait, retweet_redis_dict, comment_redis_dict
from time_utils import ts2datetime, datetime2ts

begin_ts = datetime2ts(R_BEGIN_TIME)
index_name = "user_portrait_network"
index_type = "network"

def get_es_num(timestamp):
    date = ts2datetime(timestamp)
    date_ts = datetime2ts(date)
    num = int((timestamp - date_ts) / (3 * HOUR))
    return num

def save_pr_results(sorted_uids, es_num):
    bulk_action = []
    for uid, rank in sorted_uids:
        user_results = {}
        user_results['uid'] = uid
        user_results['pr_'+str(es_num)] = rank
        if es_num == 0:
            action = {'index':{'_id':uid}}
            bulk_action.extend([action,{'doc':user_results}])
        else:
            item_exist = es_user_portrait.get(index=index_name, doc_type=index_type, id=uid)['_source']
            if item_exist:
                action = {'update':{'_id':uid}}
                try:
                    # update
                    pr_last = item_exist['pr_'+str(es_num-1)]
                except:
                    # index
                    pr_last = 0
            else:
                action = {'index':{'_id':uid}}
                pr_last = 0
            
            user_results['pr_diff_'+str(es_num)] = rank - pr_last
            bulk_action.extend([action,{'doc':user_results}])

    #print bulk_action
    es_user_portrait.bulk(bulk_action, index=index_name, doc_type=index_type)

#use to get db_number which is needed to es
def get_db_num(timestamp):
    date = ts2datetime(timestamp)
    date_ts = datetime2ts(date)
    db_number = 2 - (((date_ts - begin_ts) / (DAY * 7))) % 2
    #run_type
    if RUN_TYPE == 0:
        db_number = 1
    return db_number

def write_tmp_file(tmp_file, uid, item_result):
    count = 0
    for key in item_result:
        if key != 'None':
            count += 1
            tmp_file.write('%s\t%s\t%s\n' % (uid, key, item_result[key]))

    tmp_file.flush()
    #print 'write tmp line count:', count

def scan_retweet(tmp_file):
    count = 0
    scan_cursor = 0
    now_ts = time.time()
    now_date_ts = datetime2ts(ts2datetime(now_ts))
    #get redis db number
    db_number = get_db_num(now_date_ts)
    #get redis db
    #retweet_redis = retweet_redis_dict[str(db_number)]
    retweet_redis = comment_redis_dict[str(db_number)]
    start_ts = time.time()
    while count < 10:
        re_scan = retweet_redis.scan(scan_cursor, count=100)
        re_scan_cursor = re_scan[0]
        for item in re_scan[1]:
            count += 1
            item_list = item.split('_')
            if len(item_list)==2:
                uid = item_list[1]
                item_result = retweet_redis.hgetall(item)
                write_tmp_file(tmp_file, uid, item_result)
        end_ts = time.time()
        #run_type
        # if RUN_TYPE == 0:
            #print '%s sec scan %s count user:' %(end_ts - start_ts, count)
        start_ts = end_ts
        scan_cursor = re_scan[0]
        if scan_cursor==0:
            break
    print 'total %s sec scan %s count user:' %(end_ts - now_ts, count)


