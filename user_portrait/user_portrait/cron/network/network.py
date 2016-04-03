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
from utils import scan_retweet, save_dg_pr_results, get_es_num
from time_utils import ts2datetime, datetime2ts, ts2date

def pagerank_rank():
    timestamp = time.time()
    es_num = get_es_num(timestamp)
    if es_num == 0:
        network_es_mappings()

    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    print 'step 1', ts2date(timestamp)
    scan_retweet(tmp_file)
    tmp_file.close()
    if not tmp_file:
        return
    input_tmp_path = tmp_file.name
    print input_tmp_path

    ITER_COUNT = 10
    TOP_N = 50
    print 'step 2', ts2date(time.time())
    dg_sorted_uids, pr_sorted_uids = pagerank(ITER_COUNT, input_tmp_path, TOP_N)
    print 'step 3', ts2date(time.time())
    save_dg_pr_results(dg_sorted_uids, es_num, 'dg')    
    save_dg_pr_results(pr_sorted_uids, es_num, 'pr')    

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

    lines = sc.textFile(tmp_file_path, 1)

    initials = lines.map(lambda urls: parseNeighbors(urls)).distinct().cache() # (uid_a,(uid_b, num))
    user_ranks = initials.map(lambda (url, neighbors): (url, neighbors[1])).reduceByKey(add) #(uid_a, num)
    extra_ranks = initials.values().reduceByKey(add).cache() #(uid_b, num)

    degrees = user_ranks.union(extra_ranks).reduceByKey(add).cache()    # (uid, degree)

    links = initials.map(lambda (url, neighbors): (url, neighbors[0])).groupByKey().cache() #(uid_a, [uid_b,uid_c])
    init_ranks = links.map(lambda (url, neighbors): (url, 1.0))
    ranks = extra_ranks.union(init_ranks).reduceByKey(mul).cache() #(uid, rank)
    
    for iteration in xrange(int(iter_count)):
        contribs = links.join(ranks).flatMap(
            lambda (url, (urls, rank)): computeContribs(urls, rank))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    degrees_list = []
    degrees_list = degrees.sortBy(lambda x:x[1], False).collect()
    if len(degrees_list) > top_n:
        degrees_list = degrees_list[:top_n]

    results_list = []
    results_list = ranks.sortBy(lambda x:x[1], False).collect()
    if len(results_list) > top_n:
        results_list = results_list[:top_n]

    f = open("degree.txt", "w")
    for uid, r in degrees_list:
        # sorted_uids.append(uid)
        # print '%s\t%s\n' % (uid, r)
        print >> f, '%s\t%s\n' % (uid, r)
    f.close()
    f = open("rank.txt", "w")
    for uid, r in results_list:
        print >> f, '%s\t%s\n' % (uid, r)
    f.close()
    # delete file
    #os.remove(prefix_name + file_name)
    sc.stop()
    return degrees_list, results_list



if __name__ == '__main__':
    pagerank_rank()

