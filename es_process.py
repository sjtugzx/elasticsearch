import elasticsearch

import operation

if __name__ == '__main__':

    host = "10.10.10.10:9200"
    es = elasticsearch.Elasticsearch(host)
    print("es.ping(): ", es.ping())
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
        print(context)
        print('=' * 50 + '%s' % cnt + '=' * 50)
        # data={'context':paper_context}
        # operation.create_es_index(host='10.10.10.10:9200',index='acm', id=cnt, body=data)
