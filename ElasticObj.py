from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers


class ElasticObj(object):
    def __init__(self, index_name, ip='127.0.0.1'):
        self.index_name = index_name
        self.es = Elasticsearch([ip], port=9200)

    def create_index(self):
        index_setting = {
            "settings": {
                "index": {
                    "number_of_shards": "16",
                    "number_of_replicas": "0"
                },
                "analysis": {
                    "filter": {
                        "my_stopwords": {
                            "type": "stop",
                            "stopwords": ["the", "a"]
                        },
                        "eng_stemmer": {
                            "type": "stemmer",
                            "name": "english"
                        },
                        "eng_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
                        }
                    },
                    "analyzer": {
                        "acm_paper_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "my_stopwords", "eng_stemmer", "eng_stop", "asciifolding"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "context": {
                        "type": "text",
                        "index": True,
                        "analyzer": "acm_paper_analyzer",
                        "search_analyzer": "acm_paper_analyzer"
                    }
                }
            }
        }
        if not self.es.indices.exists(index=self.index_name):
            new_index = self.es.indices.create(index=self.index_name, body=index_setting)
            print("es.ping(): ", self.es.ping())
            print(new_index)
        else:
            print("this index has already been created!!!")

    def delete_index(self):
        deleted_index = self.es.indices.delete(index=self.index_name)
        print(deleted_index)

    def delete_index_data(self, pid):
        deleted_index = self.es.delete(index=self.index_name, id=pid)
        print(deleted_index)

    def bulk_index_data(self, dataset):
        ACTIONS = []
        i = 1
        for data in dataset:
            action = {
                "_index": self.index_name,
                "_id": i,  # _id 也可以默认生成，不赋值
                "_source": {
                    "context": data['context']
                }
            }
            i += 1
            ACTIONS.append(action)
        print(ACTIONS[0])
        insert_index = helpers.bulk(self.es, ACTIONS)
        # print(insert_index)

    def search(self, info):
        search_body = {
            "query": {
                "match_phrase": {
                    "context": {
                        "query": info,
                        "slop": 4
                    }
                }
            }
        }
        result = self.es.search(index=self.index_name, body=search_body)
        # print(result)
        for hit in result['hits']['hits']:
            print(hit['_source']['context'])
