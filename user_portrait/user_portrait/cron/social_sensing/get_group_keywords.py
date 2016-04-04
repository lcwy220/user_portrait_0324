# -*-coding:utf-8-*-

import sys
import json
import time
from config import load_scws, load_dict, cut_filter, re_cut
reload(sys)
sys.path.append('../../')
from global_utils import es_user_portrait, es_user_profile, es_flow_text
from global_utils import profile_index_name, profile_index_type, portrait_index_name, portrait_index_type, \
                         flow_text_index_name_pre, flow_text_index_type
from time_utils import ts2datetime, datetime2ts
from parameter import DAY

sw = load_scws()
#cx_dict = set(['Ag','a','an','Ng','n','nr','ns','nt','nz','Vg','v','vd','vn','@','j'])
cx_dict = set(['Ng','n','nr','ns','nt','nz']) # 关键词词性词典, 保留名词

# 1. 获取用户最近两天的微博，抽取关键词作为监测目标
def get_group_keywords(uid_list):
    now_ts = time.time()
    now_ts = datetime2ts('2013-09-03')
    former_ts = now_ts - DAY
    flow_index_1 = flow_text_index_name_pre + ts2datetime(now_ts)
    flow_index_2 = flow_text_index_name_pre + ts2datetime(former_ts)
    query_body = {
        "query":{
            "filtered":{
                "filter":{
                    "terms":{
                        "uid":uid_list
                    }
                }
            }
        },
        "size":10000
    }
    text_list = [] # 为分词前的文本
    word_dict = dict() # 分词后的word dict
    text_results = es_flow_text.search(index=[flow_index_1, flow_index_2], doc_type=flow_text_index_type, body=query_body)["hits"]["hits"]
    if text_results:
        for item in text_results:
            iter_text = item['_source']['text'].encode('utf-8', "ignore")
            iter_text = re_cut(iter_text)
            text_list.append(iter_text)
    if text_list:
        for iter_text in text_list:
            cut_text = sw.participle(iter_text)
            cut_word_list = [term for term, cx in cut_text if cx in cx_dict]
            tmp_list = []
            for w in cut_word_list:
                if word_dict.has_key(w):
                    word_dict[w] += 1
                else:
                    word_dict[w] = 1

    return word_dict


def query_keyword(keyword):
    query_body = {
        "query":{
            "filtered":{
                "filter":{
                    "term":{
                        "keywords_string":

if __name__ == "__main__":
    key_list = get_group_keywords(["3575186384", "1316683401", "1641542052"])
    for k,v in key_list.iteritems():
        if len(k) > 3 and v>1:
            print k,v

