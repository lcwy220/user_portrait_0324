# -*- coding: UTF-8 -*-
import gensim
from gensim import corpora, models, similarities
import time
import datetime
import csv
import heapq
from operator import itemgetter, attrgetter
from lda_config import load_scws,cx_dict,single_word_whitelist,black_word,re_cut

class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []
 
    def Push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0][0]
            if elem[0] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]

def get_data(weibo):#分词

    sw = load_scws()

    uid_word = []
    for text in weibo:
        v = re_cut(text)
        words = sw.participle(v)
        word_list = []
        for word in words:
            if (word[1] in cx_dict) and 3 < len(word[0]) < 30 and (word[0] not in black_word) and (word[0] not in single_word_whitelist):#选择分词结果的名词、动词、形容词，并去掉单个词
                word_list.append(word[0])
        uid_word.append(word_list)
    
    return uid_word


def lda_main(texts,nt):
    '''
        lda方法主函数：
        输入数据：
        texts：list对象，一条记录表示一个用户发布
    '''

    ##生成字典
    dictionary=corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=5, no_above=0.5, keep_n=None)
    corpus = [dictionary.doc2bow(text) for text in texts]

    ##生成tf-idf矩阵
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]

    ##LDA模型训练
    lda = gensim.models.ldamodel.LdaModel(corpus=corpus_tfidf, id2word=dictionary, num_topics=nt, update_every=1, chunksize=5000, passes=1)

    ##将对应的topic写入文件
    topics=lda.show_topics(num_topics=nt, num_words=10, log=False, formatted=True)
    
    t_topic = []
    for t in topics:
        word_list = t[1].encode('utf-8')
        t_topic.append(word_list)

    return t_topic

def topic_lda_main(weibo,nt):
    '''
        主函数：
        输入数据：微博list，主题数量
    '''
    
    texts = get_data(weibo)
    topics = lda_main(texts,nt)

    return topics


if __name__ == '__main__':

    weibo = input_data()
    start = time.time()
    topics = topic_lda_main(weibo,5)
    end = time.time()
    print 'it takes %s seconds...' % (end-start)
    start = time.time()
    topics = topic_lda_main(weibo,10)
    end = time.time()
    print 'it takes %s seconds...' % (end-start)

