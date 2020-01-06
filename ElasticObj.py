from elasticsearch import Elasticsearch, RequestsHttpConnection
class ElasticObj(object):
    def __init__(self, index_name,index_type, ip='127.0.0.1'):
        self.index_name=index_name
        self.index_type=index_type
        self.es=Elasticsearch([ip],port=9200)

    def create_index(self, index_name, index_type):
        _index_mappings = {
            "mappings": {
                index_type: {  # 相当于数据库中的表名
                    "properties": {
                        "context":{
                            "type":"string",
                            "index":True,
                            "store":True
                        }
                    }
                }
            }
        }

    def delete_index_data(self,id):
        res=self.es.delete(index=self.index_name, doc_type=self.index_type, id=id)
        print(res)

    def bulk_index_data(self,dataset):
        ACTIONS=[]
        i=1
        for data in dataset:
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_id": i,  # _id 也可以默认生成，不赋值
                "_source": {
                    # "title":dataset['title'],
                    # "author":dataset['author'],
                    # "date":dataset['date'],
                    "context":dataset['context']
                }

            }

