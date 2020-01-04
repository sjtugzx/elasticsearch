import elasticsearch

import operation

if __name__ == '__main__':

    host = "10.10.10.10:9200"
    es = elasticsearch.Elasticsearch(host)
    create_acm_index = es.indices.create(index='acm', ignore=400)
    print("es.ping(): ", es.ping())
    print(create_acm_index)
    cnt = 0
    while cnt < 10:
        cnt += 1

        file_path = '/Users/troykuo/Desktop/acm_papers/%s.pdf' % cnt
        with open(file_path, 'rb') as pdfIO:
            paper_context = operation.parse(pdfIO, '/Users/troykuo/Desktop/acm_papers/%s.txt' % cnt)
        context=""
        try:
            with open('/Users/troykuo/Desktop/acm_papers/%s.txt' % cnt, 'r') as f:
                context = f.read().replace('\n',' ')
        except FileNotFoundError:
            print("No such file or directory:"+'/Users/troykuo/Desktop/acm_papers/%s.txt' % cnt)
        # print(context)
        print('=' * 50 + '%s' % cnt + '=' * 50)
        data={'context':paper_context}
        # operation.create_es_index(host='10.10.10.10:9200',index='acm', id=cnt, body=data)
        # es.indices.delete(index='acm')
        post_index=es.create(index='acm', doc_type='paper', id=cnt, body=data)
        print(post_index)
