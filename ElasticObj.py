from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers


class ElasticObj(object):
    def __init__(self, index_name, index_type, ip='127.0.0.1'):
        self.index_name = index_name
        self.index_type = index_type
        self.es = Elasticsearch([ip], port=9200)

    def create_index(self, index_name, index_type):
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
                            "filter": ["lowercase", "my_stopwords","eng_stemmer","eng_stop","asciifolding"]
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
        if not self.es.indices.exists(index=index_name):
            new_index = self.es.indices.create(index=index_name, body=index_setting)
            print(new_index)

    def delete_index_data(self, id):
        deleted_index = self.es.delete(index=self.index_name, doc_type=self.index_type, id=id)
        print(deleted_index)

    def bulk_index_data(self, dataset):
        ACTIONS = []
        i = 1
        for data in dataset:
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_id": i,  # _id 也可以默认生成，不赋值
                "_source": {
                    # "title":dataset['title'],
                    # "author":dataset['author'],
                    # "date":dataset['date'],
                    "context": dataset['context'].decode('utf8')
                }
            }
            i+=1
            ACTIONS.append(action)
        insert_index=helpers.bulk(self.es,ACTIONS)

