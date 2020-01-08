import operation
from ElasticObj import ElasticObj

if __name__ == '__main__':
    acm_es = ElasticObj('acm')
    acm_es.create_index()
    cnt = 1
    dataset = []
    while cnt <= 10:
        f_prefix = "data"
        filepath = f_prefix + '/%s.txt' % cnt
        data = operation.get_context(filepath)
        if not (data == ''):
            context = {'context': data}
            dataset.append(context)
            print(context)
        cnt += 1
    acm_es.bulk_index_data(dataset)
    print('='*150)
    # searching_text = "Optimal Integration of Inter-Task"
    # st1=operation.es_search('acm',searching_text)
    # print(operation.string_similar(searching_text, st1))
    # acm_es.search(searching_text)
    operation.similarity_checking('acm', "data/text.txt")
    # operation.similarity_checking('acm', "data/3.txt")

