import operation
from ElasticObj import ElasticObj

if __name__ == '__main__':
    acm_es = ElasticObj('acm','paper')
    acm_es.delete_index()
    acm_es.create_index()
    cnt = 1
    dataset = []
    while cnt <= 10:
        f_prefix = "/Users/troykuo/Desktop/acm_papers"
        filepath = f_prefix + '/%s.txt' % cnt
        data = operation.get_context(filepath)
        if not (data == ''):
            context = {'context': data}
            dataset.append(context)
            print(context)
        cnt += 1
    acm_es.bulk_index_data(dataset)
    searching_text = "Single Scattering"
    acm_es.search(searching_text)
