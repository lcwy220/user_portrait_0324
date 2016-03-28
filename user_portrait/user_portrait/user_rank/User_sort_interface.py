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
            during = ( datetime2ts(et) - datetime2ts(st) ) / DAY + 1
            time = 1
            if during > 3:
                time = 7
            elif during > 16:
                time = 30
            search_id = add_task( username ,"keyword" , "all" ,'flow_text_' , during , st ,et, arg , sort_norm , sort_scope, time, isall)
            #deal with the offline task   
            return {"flag":True , "search_id" : search_id }
        elif sort_scope == 'all_nolimit':
            #online job
            user_list = all_sort_filter(None,sort_norm,time)
    else:
        if sort_scope == 'in_limit_keyword':
            #offline job
            #deal with the offline task
            during = ( datetime2ts(et) - datetime2ts(st) ) / DAY + 1
            time = 1
            if during > 3:
                time = 7
            elif during > 16:
                time = 30
            search_id = add_task( username ,"keyword" , "in" ,'flow_text_' , during , st ,et , arg , sort_norm , sort_scope, time, isall)
            return {"flag":True , "search_id" : search_id }
        elif sort_scope == 'in_limit_hashtag':
            during = ( datetime2ts(et) - datetime2ts(st) ) / DAY + 1
            time = 1
            if during > 3:
                time = 7
            elif during > 16:
                time = 30
            search_id = add_task( username ,"hashtag" , "in" ,'flow_text_' , during , st ,et, arg , sort_norm , sort_scope, time, isall)
            return {"flag":True , "search_id" : search_id }
        else:
            #find the scope
            user_list = in_sort_filter(time , sort_norm,sort_scope , arg)
    
    result = make_up_user_info(user_list,isall , time , sort_norm)
    return result
    
if __name__ == "__main__":    
    print json.dumps(user_sort_interface(username = "kanon", time = 1, sort_scope =  "all_limit_keyword", sort_norm = "fans" , arg = 'hello' , st = "2013-09-01"  ,et =  "2013-09-01" , isall = True) )
            
            
