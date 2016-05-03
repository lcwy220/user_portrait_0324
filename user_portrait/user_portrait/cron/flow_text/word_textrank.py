#-*-coding=utf-8-*-

import os
import re
import sys
import json
import csv
import codecs
import jieba.analyse
from utils import black_word,cut_filter
#from textrank4zh import TextRank4Keyword, TextRank4Sentence
zhPattern = re.compile(u'[\u4e00-\u9fa5]+')

def get_keyword(tweet,tr4w):

    if isinstance(tweet['text'],unicode):
        text = tweet['text'].encode('utf-8')
    else:
        text = tweet['text']

    tr4w.analyze(text=text, lower=True, window=2)
    k_dict = tr4w.get_keywords(10, word_min_len=2)
    word_list = dict()
    for item in k_dict:
        word_list[item.word.encode('utf-8')] = text.count(item.word.encode('utf-8'))

    return word_list
    #return k_dict

def get_keywords_jieba(tweet):

    if isinstance(tweet['text'],unicode):
        text = tweet['text'].encode('utf-8')
    else:
        text = tweet['text']

    w_text = cut_filter(text)
    tags = jieba.analyse.extract_tags(w_text,10)

    word_list = dict()
    for t in tags:
        if t not in black_word and zhPattern.search(t):
            word_list[t] = text.count(t)

    return word_list


    




        
