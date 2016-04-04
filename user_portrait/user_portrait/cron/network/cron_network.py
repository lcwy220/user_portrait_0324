# -*- coding:utf-8 -*-
import os
import tempfile
import re
import sys
import json
import time
import tempfile
import shutil
reload(sys)
sys.path.append('../../')
from operator import add, mul
from pyspark import SparkContext
from time_utils import ts2datetime, datetime2ts, ts2date
from keywords_utils import get_task_information, identify_task_exist,\
        compute_network_task, write_tmp_file, save_task_results,\
        push_task_information

#use to read task information from queue
def scan_network_keywords_task():
    #step1: read task information from redis queue
    #step2: identify the task information is exist in es
    #step3: compute the network trend task
    while True:
        #read task informaiton from redis queue
        network_task_information = get_task_information()
        print network_task_information
        #when redis queue null - file break
        if not network_task_information:
            break
        #identify the task is exist in es
        exist_mark = identify_task_exist(network_task_information)
        print 'exist_mark:', exist_mark
        if exist_mark:
            print 'step 1: compute', ts2date(time.time())
            results = compute_network_task(network_task_information)
            if results:
                tmp_file = tempfile.NamedTemporaryFile(delete=False)
                write_tmp_file(tmp_file, results)
                tmp_file.close()

                if not tmp_file:
                    return
                input_tmp_path = tmp_file.name
                print input_tmp_path

                ITER_COUNT = 10
                TOP_N = 50
                print 'step 2: pagerank', ts2date(time.time())
                dg_sorted_uids, pr_sorted_uids = pagerank(ITER_COUNT, input_tmp_path, TOP_N, 'keywords')
                #save results
                print 'step 3: save', ts2date(time.time())
                save_mark = save_task_results(dg_sorted_uids, pr_sorted_uids, network_task_information)
                print 'save done', ts2date(time.time())
                #identify save status
                if not save_mark:
                    #status fail: push task information to redis queue
                    push_mark = push_task_information(network_task_information)
                    if not push_mark:
                        print 'error push task queue'
        else:
            #if no exist - pass
            pass

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], (parts[1], int(parts[2]))

def parseNeighborsKeywords(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return (parts[0], parts[1])

def pagerank(iter_count, input_file, top_n, flag):
    '''
    if not (iter_count and input_file and os.path.exists(input_file)):
        print 'error'
        return []
    '''
    prefix_name = '/mnt/mfs/'
    file_name = input_file.split('/')[-1]
    tmp_file_path = os.path.join("file://" + prefix_name, file_name )

    shutil.copy(input_file, prefix_name)
    try:
        sc = SparkContext(appName=file_name,master="mesos://219.224.134.213:5050")
    except:
        print 'service unavailable'
        return
    lines = sc.textFile(tmp_file_path, 1)
    
    if flag == 'keywords': #keywords
        rdd_for_reduce = lines.map(lambda urls: (parseNeighborsKeywords(urls), 1.0)).reduceByKey(add) # ((uid_a,uid_b), num)
        initials = rdd_for_reduce.map(lambda ((uid_a, uid_b), num): (uid_a, (uid_b, num))).cache() # (uid_a, (uid_b, num))
    else:  #all
        initials = lines.map(lambda urls: parseNeighbors(urls)).distinct().cache() # (uid_a,(uid_b, num))
    

    user_ranks = initials.map(lambda (url, neighbors): (url, neighbors[1])).reduceByKey(add) #(uid_a, num)
    extra_ranks = initials.values().reduceByKey(add).cache() #(uid_b, num)

    degrees = user_ranks.union(extra_ranks).reduceByKey(add).cache()    # (uid, degree)
 
    degrees_list = []
    degrees_list = degrees.sortBy(lambda x:x[1], False).collect()
    if len(degrees_list) > top_n:
        degrees_list = degrees_list[:top_n]
    
    all_uids = initials.flatMap(lambda (url, neighbors): [url, neighbors[0]]).distinct()
    all_uids_map = all_uids.flatMap(lambda x: [('global', x), (x, 'global')])
    global_links = all_uids_map.groupByKey()
    
    ini_links = initials.map(lambda (url, neighbors): (url, neighbors[0])).groupByKey() #(uid_a, [uid_b,uid_c])
    links = global_links.union(ini_links).cache()
    init_ranks = links.map(lambda (url, neighbors): (url, 1.0))
    ranks = extra_ranks.union(init_ranks).reduceByKey(mul).cache() #(uid, rank)
    for iteration in xrange(int(iter_count)):
        contribs = links.join(ranks).flatMap(
            lambda (url, (urls, rank)): computeContribs(urls, rank))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    results_list = []
    results_list = ranks.sortBy(lambda x:x[1], False).collect()

    #exclude global
    top_n += 1
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

if __name__=='__main__':
    log_time_ts = time.time()
    log_time_date = ts2datetime(log_time_ts)
    print 'cron/network/cron_network.py&start&' + log_time_date
    
    scan_network_keywords_task()

    log_time_ts = time.time()
    log_time_date = ts2datetime(log_time_ts)
    print 'cron/network/cron_network.py&end&' + log_time_date
