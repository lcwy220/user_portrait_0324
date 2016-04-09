# -*-coding:utf-8-*-
import time
import sys
import redis
import numpy as np
from elasticsearch import Elasticsearch
reload(sys)
sys.path.append('../../../')
from global_utils import es_user_portrait as es_9200
from global_utils import flow_text_index_name_pre, flow_text_index_type
from parameter import DAY, RUN_TYPE

BCIHIS_INDEX_NAME = 'bci_history'
BCIHIS_INDEX_TYPE = 'bci'


def var_cal(n , old_var , old_ave , new_ave , new_bci):
    return ( n - 1) / n * (old_var + np.power(old_ave - new_ave , 2) ) + np.power(new_bci - new_ave , 2) / n

def cal_num_for_bci_history(id , total_num , today_bci ,update_time):
    query = {"query":{"bool":{"must":[{"term":{"bci.uid":id}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}
    if 1:
        result = es_9200.search(index = BCIHIS_INDEX_NAME , doc_type = BCIHIS_INDEX_TYPE , body = query )['hits']
        item = {}
        if result['total'] > 0:    
            during = result['hits'][0]['_source']['during']
            uid = id
            bci_day_last = result['hits'][0]['_source']['bci_day_last']
            bci_day_change = result['hits'][0]['_source']['bci_day_change']
            bci_day_var = result['hits'][0]['_source']['bci_day_var'] 
            bci_day_ave = result['hits'][0]['_source']['bci_day_ave'] 
            bci_week_num = result['hits'][0]['_source']['bci_week_num']
            bci_week_change = result['hits'][0]['_source']['bci_week_change']
            bci_week_var = result['hits'][0]['_source']['bci_week_var'] 
            bci_week_ave = result['hits'][0]['_source']['bci_week_ave'] 
            bci_month_num = result['hits'][0]['_source']['bci_month_num']
            bci_month_change = result['hits'][0]['_source']['bci_month_change']
            bci_month_var = result['hits'][0]['_source']['bci_month_var'] 
            bci_month_ave = result['hits'][0]['_source']['bci_month_ave']

            weibo_day_last = result['hits'][0]['_source']['bci_day_last']
            weibo_week_num = result['hits'][0]['_source']['weibo_week_num']
            weibo_month_num = result['hits'][0]['_source']['weibo_month_num']
            
            item['bci_day_last'] = today_bci 
            item['bci_day_change'] = today_bci - bci_day_last
            item['bci_day_ave'] = (bci_day_ave * n + today_bci) / ( n + 1)
            item['bci_day_var'] = var_cal(n+1 , bci_day_var , bci_day_ave , item['bci_day_ave'] ,today_bci )
            if n < 7:
                week_dividend = n 
            else:
                week_dividend = 7
            
            item['bci_week_sum'] = bci_week_num - bci_week_ave + today_bci
            item['bci_week_change'] = item['bci_week_sum'] - bci_week_num
            item['bci_week_ave'] = item['bci_week_sum'] / week_dividend
            item['bci_week_var'] = var_cal( week_dividend , bci_week_var , bci_week_ave , item['bci_week_ave'] ,today_bci )
            
            if n < 30:
                month_dividend = n 
            else:
                month_dividend = 30
            item['bci_month_sum'] = bci_month_num - bci_month_ave + today_bci
            item['bci_month_change'] = item['bci_month_sum'] - bci_week_num
            item['bci_month_ave'] = item['bci_month_sum'] / month_dividend
            item['bci_month_var'] = var_cal( month_dividend , bci_month_var , bci_month_ave , item['bci_month_ave'] ,today_bci )
            
            item['weibo_day_last'] = weibo_day_last
            if n <= 7:
                item['weibo_week_sum'] = weibo_week_num + total_num
            else :
                item['weibo_week_sum'] = weibo_week_num / 7 * 6 + total_num

            if n <= 30:
                item['weibo_month_sum'] = weibo_month_sum + total_num
            else:
                item['weibo_month_sum'] = weibo_month_sum / 30 * 29 + total_num

            item['during'] = n + 1
            item['update_time'] = update_time
            return item
        else:
            item['bci_day_last'] = today_bci 
            item['bci_day_change'] = today_bci
            item['bci_day_ave'] = today_bci
            item['bci_day_var'] = 0
            item['bci_week_sum'] = today_bci
            item['bci_week_change'] = today_bci
            item['bci_week_ave'] = today_bci
            item['bci_week_var'] = 0            
            item['bci_month_sum'] = today_bci
            item['bci_month_change'] = today_bci
            item['bci_month_ave'] = today_bci
            item['bci_month_var'] = 0           
            item['weibo_day_last'] = total_num     
            item['weibo_week_sum'] = total_num 
            item['weibo_month_sum'] = total_num 
            item['during'] = 1
            item['update_time'] = update_time
            return item
