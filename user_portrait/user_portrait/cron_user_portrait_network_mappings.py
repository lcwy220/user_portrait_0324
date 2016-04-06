from elasticsearch import Elasticsearch
from global_utils import ES_CLUSTER_FLOW1 as es

def network_count_es_mappings():
    index_info = {
        "settings":{
            "analysis":{
                "analyzer":{
                    "my_analyzer":{
                        "type":"pattern",
                        "pattern":"&"
                    }
                }
            }
        },
        "mappings":{
            "network":{
                "properties":{
                    "start_date":{
                        "type": "string",
                        "index" : "not_analyzed"
                    }
                }
            }
        }
    }
    index_name = 'user_portrait_network_count'
    exist_bool = es.indices.exists(index=index_name)
    print exist_bool
    if exist_bool:
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name, body=index_info, ignore=400)
    return True

def network_es_mappings():
    index_info = {
        "settings":{
            "analysis":{
                "analyzer":{
                    "my_analyzer":{
                        "type":"pattern",
                        "pattern":"&"
                    }
                }
            }
        },
        "mappings":{
            "network":{
                "properties":{
                    "uid":{
                        "type": "string",
                        "index" : "not_analyzed"
                    }
                }
            }
        }
    }
    index_name = 'user_portrait_network'
    exist_bool = es.indices.exists(index=index_name)
    print exist_bool
    if exist_bool:
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name, body=index_info, ignore=400)
    return True

if __name__ == '__main__':
    network_es_mappings()
    #network_count_es_mappings()
