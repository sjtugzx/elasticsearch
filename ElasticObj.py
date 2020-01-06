from elasticsearch import Elasticsearch
from elasticsearch import helpers


class ElasticObj(object):
    def __init__(self, index_name,host='127.0.0.1'):
        self.index_name = index_name
        self.es = Elasticsearch([{'host': host, 'port': '9200'}])
        print("es.ping(): ", self.es.ping())

    def create_index(self):
        '''
        It is used for creating new index.
        '''
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
            print(new_index)
        else:
            print("this index has already been created!!!")

    def delete_index(self):
        '''
        It is used to delete useless index
        '''
        deleted_index = self.es.indices.delete(index=self.index_name)
        print(deleted_index)

    def delete_index_data(self, pid):
        '''
        It is used to delete the data with specific id
        '''
        deleted_index = self.es.delete(index=self.index_name, id=pid)
        print(deleted_index)

    def bulk_index_data(self, dataset):
        '''
        It is used to create index in batch
        :param dataset: the dataset for creating index
        '''
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

        insert_index = helpers.bulk(self.es, ACTIONS)
        print("Create %s indexes successfully!!!" % len(ACTIONS))

    def search(self, info):
        '''
        It is used to search the information in es
        :param info: text for searching
        :return: searching result
        '''
        searching_body = {
            "query": {
                "match_phrase": {
                    "context": {
                        "query": info,
                        "slop": 4
                    }
                }
            }
        }
        searched = self.es.search(index=self.index_name, body=searching_body)
        print(type(searched))
        print(searched)
        for hit in searched['hits']['hits']:
            print(hit['_source'])
