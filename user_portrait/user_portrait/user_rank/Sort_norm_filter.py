#-*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
import sys

BCI_INDEX_NAME = 'copy_user_portrait_influence'
BCI_INDEX_TYPE = 'bci'

ACT_INDEX_NAME = 'copy_user_portrait_activeness'
ACT_INDEX_TYPE = 'activeness'

IMP_INDEX_NAME = 'copy_user_portrait_importance'
IMP_INDEX_TYPE = 'importance'

SES_INDEX_NAME = 'copy_user_portrait_sensitive'
SES_INDEX_TYPE = 'sensitive'

BCIHIS_INDEX_NAME = 'bci_history'
BCIHIS_INDEX_TYPE = 'bci'

SESHIS_INDEX_NAME = 'sensitive_history'
SESHIS_INDEX_TYPE = 'sensitive'


es = Elasticsearch(['219.224.134.213', '219.224.134.214'], timeout = 6000)

def sort_norm_filter(uid_list = [] , sort_norm = 'imp' , time = 1 , isall = False):   
    uid = []
    if sort_norm == 'bci':
        if isall :
            uid = history_sort('bci_',BCIHIS_INDEX_NAME,BCIHIS_INDEX_TYPE,uid_list,time,False)
        else:
            uid = history_sort('bci_',BCI_INDEX_NAME,BCI_INDEX_TYPE,uid_list,time,False)
    elif sort_norm == 'bci_change':
        if isall :
            uid = history_sort('bci_',BCIHIS_INDEX_NAME,BCIHIS_INDEX_TYPE,uid_list,time,True)
        else:
            uid = history_sort('bci_',BCI_INDEX_NAME,BCI_INDEX_TYPE,uid_list,time,True)
    elif sort_norm == 'ses':
        if isall :
            uid = history_sort('ses_',SESHIS_INDEX_NAME,SESHIS_INDEX_TYPE,uid_list,time,False)
        else :
            uid = history_sort('ses_',SES_INDEX_NAME,SES_INDEX_TYPE,uid_list,time,False)
    elif sort_norm == 'ses_change':
        if isall :
            uid = history_sort('ses_',SESHIS_INDEX_NAME,SES_HISTORY_TYPE,uid_list,time,True)
        else :
            uid = history_sort('ses_',SES_INDEX_NAME,SES_INDEX_TYPE,uid_list,time,True)
    elif sirt_norm == 'imp':
        uid = history_sort('imp_',IMP_HISTORY_INDEX,IMP_HISTORY_TYPE,uid_list,time,False)
    elif sort_norm == 'imp_change':
        uid = history_sort('imp_',IMP_HISTORY_INDEX,IMP_HISTORY_TYPE,uid_list,time,True )
    elif sirt_norm == 'act':
        uid = history_sort('act_',ACT_HISTORY_INDEX,ACT_HISTORY_TYPE,uid_list,time,False)
    elif sort_norm == 'act_change':
        uid = history_sort('act_',ACT_HISTORY_INDEX,ACT_HISTORY_TYPE,uid_list,time,True )
    return uid


def history_sort( prefix ,index_name , index_type , uid_list , time , ischange = False):
    sort_field = prefix
    if time == 1 :
        if ischange:
            sort_field += "day_change"
        else:
            sort_field += "day_last"
    elif time == 7:
        if ischange:
            sort_field += "week_change"
        else:
            sort_field += "week_ave"
    else:
        if ischange:
            sort_field += "month_change"
        else:
            sort_field += "month_ave"

    query = {
                "query": {
                    "filtered": {
                        "filter": {
                            "terms": {
                                "uid": uid_list
                            }
                        }       
                    }           
                },
                "sort": [{ sort_field : { "order": "desc" } }],
                "fields" : []
            }
    try:
        result = es.search(index = index_name , doc_type = index_type , body = query)['hits']['hits']
        uid_list = []
        for item in result :
            uid_list.append(item['_id'].encode("utf-8") )
        return uid_list
    except Exception,e:
        print e
        raise  Exception('get history_sort user_list failed!')

    
    