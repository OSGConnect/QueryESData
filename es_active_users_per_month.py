#!/usr/bin/env python

import sys
import argparse
import json
import unicodedata
import re

import elasticsearch
import elasticsearch.helpers

ES_NODES = ['uct2-es-head.mwt2.org:9200', 'uct2-es-door.mwt2.org:9200']

def print_es_doc(es_doc, es_doc_index):
    """ Print the values from es document 
      
        arguments:
        es_doc -- the document in dictionary format obtained from elasticsearch 
        es_doc_index -- the elastic search index  

        return: None

    """

    doc_hits = es_doc['hits']['total']
    count_k = 0 
    count_k_doc = 0
    for k in es_doc['aggregations']['users_count']['buckets']:
         if k['doc_count'] > 0:
             print "user_id= {0:13s}    jobs= {1:12d}    es_db=  {2:40s}".\
                format(k['key'], k['doc_count'], es_doc_index)
             count_k = count_k+1
             count_k_doc += int(k['doc_count'])

    print("--"*50)
    print "tot_users= {0:3d} tot_jobs= {1:9d} tot_hits= {2:12d} \
           es_db= {3:40s}".format(count_k, count_k_doc, doc_hits, es_doc_index)
    print("--"*50)


def get_active_users(es, es_ind):
    """ Elastic search querry for active users for a given index

        arguments:
        es -- the elasticsearch object of database server
        es_ind -- the index of elastic search object 

        return: active users who submitted jobs

    """
    if es is None:
        return

    es_data = es.search(es_ind, body= {
                  "size":0,
                  "aggregations": {
                      "users_count": {
                          "terms": {
                             "field": "Owner",
                             "size":0 
                           }
                        }
                   }
                })
    print_es_doc(es_data, es_ind)


def get_osg_history_index_list(es=None):
    """ Get the list of osg-history-index 

        arguments:
        es -- elasticsearch object 

        return: list of indices matching osg-connect-job-history 

    """
    if es is None:
        return

    all_indices = elasticsearch.client.CatClient(es).indices()
    all_indices_string_split = all_indices.encode('ascii', 'replace').split(' ')

    osg_index = []
    for word in all_indices_string_split:
        if word.startswith('condor_history-20'):
            osg_index.append(word)
    osg_index.sort()
    return osg_index

def get_args():
    """ Get the argument options """
    parser = argparse.ArgumentParser(description='Get the details \
                                 of active users from ES database')
    parser.add_argument('-i','--input-index', help=
        "input index (e.g: condor_history-2016.5.5),  "
        "last = the most recent index,  "
        "all (default) = All indices are searched" )
    args_dict = vars(parser.parse_args())
    return args_dict


def get_active_users_from_list(es, es_list, option):
    """ Get the number of active users for a given index or list of indices """
    index_option = str(option['input_index'])
    if index_option.startswith('last'):
        get_active_users(es,es_list[-1])
    if index_option.startswith('condor_history-20'):
        get_active_users(es,index_option)
    if index_option.startswith('all') or index_option.startswith('None'):
        for index in es_list:
            get_active_users(es,index)


if __name__ == "__main__":
    arg_option = get_args()
    es = elasticsearch.Elasticsearch(ES_NODES)
    es_index_list = get_osg_history_index_list(es)
    get_active_users_from_list(es, es_index_list, arg_option)








