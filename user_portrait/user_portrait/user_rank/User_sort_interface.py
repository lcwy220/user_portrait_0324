#-*- coding:utf-8 -*-
from in_filter import in_sort_filter
from Offline_task import add_task
from all_filter import all_sort_filter
from Keyword_task import key_words_search
from time_utils import ts2datetime, datetime2ts
from parameter import DAY, LOW_INFLUENCE_THRESHOULD
from Makeup_info import make_up_user_info
import json

def user_sort_interface(username , time ,sort_scope , sort_norm , arg = None, st = None, et = None, isall = False):
    user_list = []
    if isall:
        #deal with the situation of all net user
        if sort_scope == 'all_limit_keyword':
            #offline job
            #add job to es index
            search_id =  add_task(username , arg ,'keyword' ,'all')
            #deal with the offline task   

            during = ( datetime2ts(st) - datetime2ts(st) ) / DAY + 1
            time = 1
            if during > 3:
                time = 7
            elif during > 16:
                time = 30
            return key_words_search(  "flow_text_" , during, st , arg ,  'all' , search_id , sort_norm  ,time , isall = True)
        elif sort_scope == 'all_nolimit':
            #online job
            user_list = all_sort_filter(None,sort_norm,time)
    else:
        if sort_scope == 'in_limit_keyword':
            #offline job
            search_id = add_task(username , arg ,'keyword' ,'all')
            #deal with the offline task
            during = ( datetime2ts(et) - datetime2ts(st) ) / DAY + 1
            time = 1
            if during > 3:
                time = 7
            elif during > 16:
                time = 30
            return key_words_search(  "flow_text_" , during, st , arg ,  'in' , search_id , sort_norm  ,time , isall = False)
        else:
            print sort_norm
            print sort_scope
            #find the scope
            user_list = in_sort_filter(time , sort_norm,sort_scope , arg)
    
    result = make_up_user_info(user_list,isall)
    return result
    
if __name__ == "__main__":
    
    print json.dumps(user_sort_interface(username = "kanon", time = 1, sort_scope =  "in_limit_topic", sort_norm = "bci" , arg = '' , st = "2013-09-01"  ,et =  "2013-09-01" , isall = False) )
            
            
