# -*- coding: utf-8 -*-

import time
import tempfile
import re
import os
import sys
import shutil
sys.path.append('../../')
from operator import add, mul
from pyspark import SparkContext
from cron_user_portrait_network_mappings import network_es_mappings
from utils import scan_retweet, save_pr_results, get_es_num
from time_utils import ts2datetime, datetime2ts

def pagerank_rank():
    timestamp = time.time()
    es_num = get_es_num(timestamp)
    if es_num == 0:
        network_es_mappings()

    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    scan_retweet(tmp_file)
    print 'pagerank start'
    if not tmp_file:
        return
    input_tmp_path = tmp_file.name
    print input_tmp_path

    ITER_COUNT = 10
    TOP_N = 50
    sorted_uids = pagerank(ITER_COUNT, input_tmp_path, TOP_N)
    save_pr_results(sorted_uids, es_num)    


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], (parts[1], int(parts[2]))

def pagerank(iter_count, input_file, top_n):
    '''
    if not (iter_count and input_file and os.path.exists(input_file)):
        print 'error'
        return []
    '''
    prefix_name = '/mnt/mfs/'
    file_name = input_file.split('/')[-1]
    tmp_file_path = os.path.join("file://" + prefix_name, file_name )

    shutil.copy(input_file, prefix_name)
    sc = SparkContext(appName=file_name,master="mesos://219.224.134.213:5050")
    # sc = SparkContext(appName=input_file)

    lines = sc.textFile(tmp_file_path, 1)

    initials = lines.map(lambda urls: parseNeighbors(urls)).distinct().cache() # (uid,(uid, num))
    extra_ranks = initials.values().reduceByKey(add) #(uid, num)
    links = initials.map(lambda (url, neighbors): (url, neighbors[0])).groupByKey().cache() #(uid, [uid,uid])
    
    init_ranks = links.map(lambda (url, neighbors): (url, 1.0))
    ranks = extra_ranks.union(init_ranks).reduceByKey(mul).cache() #(uid, num)
    
    for iteration in xrange(int(iter_count)):
        contribs = links.join(ranks).flatMap(
            lambda (url, (urls, rank)): computeContribs(urls, rank))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    results_list = []

    for (link, rank) in ranks.collect():
        results_list.append((link, rank))
    if not results_list:
        return []
    results_list = sorted(results_list, key=lambda result:result[1], reverse=True)

    if len(results_list) > top_n:
        results_list = results_list[:top_n]

    f = open("out.txt", "w")
    for uid, r in results_list:
        # sorted_uids.append(uid)
        # print '%s\t%s\n' % (uid, r)
        print >> f, '%s\t%s\n' % (uid, r)
    f.close()
    # delete file
    #os.remove(prefix_name + file_name)
    sc.stop()
    return results_list



if __name__ == '__main__':
    pagerank_rank()

