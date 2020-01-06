import elasticsearch
import operation
from ElasticObj import ElasticObj

if __name__ == '__main__':
    acm_es = ElasticObj('acm', 'paper')
    acm_es.create_index()

