#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys
import datetime
from time_utils import ts2datetime, datetime2ts
from INDEX_TABLE import *
from parameter import RUN_TYPE
from global_utils import es_user_portrait as es

USER_INDEX_NAME = 'user_portrait_1222'
USER_INDEX_TYPE = 'user'

WEBUSER_INDEX_NAME = "weibo_user"
WEBUSER_INDEX_TYPE = "user"


#es = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)

def make_up_user_info(user_list = [] , isall = False, time = 1, sort_norm = "bci" ):
    result_info = []
    
    if RUN_TYPE:
        today = str(datetime.date.today())
    else:
        today = '2013-09-07'
    timestamp = datetime2ts(today)
    #print len(user_list)
    if user_list:
        for id in user_list:
            item = {}
            if isall :
                item = all_makeup_info(id , sort_norm , time )
            else:
                item = in_makeup_info(id , sort_norm , time)
            result_info.append(item)
        return result_info
    else:
        return []

def all_makeup_info(id , sort_norm , time):
    item = {}
    query = {"query":{"bool":{"must":[{"term":{"user.uid":id}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{},"fields":["uid","nick_name","user_location","fansnum","statusnum"]}
    result = es.search(index=WEBUSER_INDEX_NAME , doc_type=WEBUSER_INDEX_TYPE , body=query)['hits']
    if result['total'] != 0 :
        item['uid'] = result['hits'][0]['fields']['uid'][0]
        item['fans'] = result['hits'][0]['fields']['fansnum'][0]
        item['location'] = result['hits'][0]['fields']['user_location'][0]
        item['uname'] = result['hits'][0]['fields']['nick_name'][0]
        item['weibo_count'] = result['hits'][0]['fields']['statusnum'][0]
    else :
        item['uid'] = None
        item['fans'] = None
        item['location'] = None
        item['uname'] = None
        item['weibo_count'] = None
    
    item['uid'] = id
    query = {"query":{"bool":{"must":[{"term":{"user.uid":id}}],"must_not":[],"should":[]}},"size":10,"sort":[],"facets":{},"fields":[]}
    result = es.search(index=USER_INDEX_NAME , doc_type=USER_INDEX_TYPE , body=query)['hits']
    if result['total'] != 0 :
        item['is_warehousing'] = True
    else :
        item['is_warehousing'] = False
    

    field_bci ,field_sen = get_all_filed(sort_norm , time) 

    item['bci'] = history_info(BCIHIS_INDEX_NAME,BCIHIS_INDEX_TYPE,id,field_bci)
    item['sen'] = history_info(SESHIS_INDEX_NAME,SESHIS_INDEX_TYPE,id,field_sen)
    return item
        

def in_makeup_info(id , sort_norm , time):
    item = {}
    query = {"query":{"bool":{"must":[{"term":{"user.uid":id}}],"must_not":[],"should":[]}},"size":10,"sort":[],"facets":{},"fields":["uid","uname","location","topic_string","domain","fansnum"]}
    result = es.search(index=USER_INDEX_NAME , doc_type=USER_INDEX_TYPE , body=query)['hits']
    if result['total'] != 0 :
        item['domain'] = result['hits'][0]['fields']['domain'][0]
        item['uid'] = result['hits'][0]['fields']['uid'][0]
        item['topic'] = result['hits'][0]['fields']['topic_string'][0]
        item['location'] = result['hits'][0]['fields']['location'][0]
        item['uname'] = result['hits'][0]['fields']['uname'][0]
        item['fans'] = result['hits'][0]['fields']['fansnum'][0]
    else :
        item['domain'] = None
        item['uid'] = None
        item['topic'] = None
        item['location'] = None
        item['uname'] = None
        item['fans'] = None

    item['uid'] = id
    field_bci , field_sen ,field_imp ,field_act = get_in_filed(sort_norm,time)
    
    item['bci'] = history_info(BCI_INDEX_NAME,BCI_INDEX_TYPE,id,field_bci)
    item['sen'] = history_info(SES_INDEX_NAME,SES_INDEX_TYPE,id,field_sen)
    item['imp'] = history_info(IMP_INDEX_NAME,IMP_INDEX_TYPE,id,field_imp)
    item['act'] = history_info(ACT_INDEX_NAME,ACT_INDEX_TYPE,id,field_act)
    return item


def history_info(index_name , index_type , uid , fields):
    
    length = len(fields)
    
    query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "uid": uid
                                }
                            }
                        ]
                    }
                },
                "fields": fields
            }
    try:
        result = es.search(index = index_name , doc_type = index_type , body = query)
        if result['timed_out'] == False and result['hits']['total'] != 0 :
            item = result['hits']['hits'][0]['fields']
            return item[fields][0]
        else :
            return None
    except Exception , e:
        print "Exception : " + str(e)
        return None


def get_all_filed(sort_norm , time):
    field_bci = 'bci_week_ave'
    field_sen = 'sensitive_week_ave'
    if sort_norm == 'bci':
        if time == '1':
            field_bci = 'bci_day_change'
        elif time == '7':
            field_bci = 'bci_week_ave'
        else:
            field_bci = 'bci_month_ave'
    elif sort_norm == 'bci_change':
        if time == '1':
            field_bci = 'bci_day_change'
        elif time == '7':
            field_bci = 'bci_week_change'
        else:
            field_bci = 'bci_month_change'
    elif sort_norm == 'ses':
        if time == '1':
            field_sen = 'sensitive_day_change'
        elif time == '7':
            field_sen = 'sensitive_week_ave'
        else:
            field_sen = 'sensitive_month_ave'
    elif sort_norm == 'ses_change':
        if time == '1':
            field_sen = 'sensitive_day_change'
        elif time == '7':
            field_sen = 'sensitive_week_change'
        else:
            field_sen = 'sensitive_month_change'
    return  field_bci, field_sen


def get_in_filed(sort_norm , time):

    field_bci = 'bci_week_ave'
    field_sen = 'sensitive_week_ave'
    field_imp = 'importance_week_ave'
    field_act = 'activeness_week_ave'

    if sort_norm == 'bci':
        if time == '1':
            field_bci = 'bci_day_change'
        elif time == '7':
            field_bci = 'bci_week_ave'
        else:
            field_bci = 'bci_month_ave'
    elif sort_norm == 'bci_change':
        if time == '1':
            field_bci = 'bci_day_change'
        elif time == '7':
            field_bci = 'bci_week_change'
        else:
            field_bci = 'bci_month_change'
    elif sort_norm == 'ses':
        if time == '1':
            field_sen = 'sensitive_day_change'
        elif time == '7':
            field_sen = 'sensitive_week_ave'
        else:
            field_sen = 'sensitive_month_ave'
    elif sort_norm == 'ses_change':
        if time == '1':
            field_sen = 'sensitive_day_change'
        elif time == '7':
            field_sen = 'sensitive_week_change'
        else:
            field_sen = 'sensitive_month_change'
    elif sort_norm == 'imp':
        if time == '1':
            field_imp = 'importance_day_change'
        elif time == '7':
            field_imp = 'importance_week_ave'
        else:
            field_imp = 'importance_month_ave'
    elif sort_norm == 'imp_change':
        if time == '1':
            field_imp = 'importance_day_change'
        elif time == '7':
            field_imp = 'importance_week_change'
        else:
            field_imp = 'importance_month_change'
    elif sort_norm == 'act':
        if time == '1':
            field_act = 'activeness_day_change'
        elif time == '7':
            field_act = 'activeness_week_ave'
        else:
            field_act = 'activeness_month_ave'
    elif sort_norm == 'act_change':
        if time == '1':
            field_act = 'activeness_day_change'
        elif time == '7':
            field_act = 'activeness_week_change'
        else:
            field_act = 'activeness_month_change'
    return  field_bci, field_sen, field_imp,field_act
      
            
     
