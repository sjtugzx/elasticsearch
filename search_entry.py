import operation
from ElasticObj import ElasticObj

if __name__ == '__main__':
    # acm_es = ElasticObj('acm')
    #     # acm_es.create_index()
    #     # cnt = 1
    #     # dataset = []
    #     # while cnt <= 10:
    #     #     f_prefix = "data"
    #     #     filepath = f_prefix + '/%s.txt' % cnt
    #     #     data = operation.get_context(filepath)
    #     #     if not (data == ''):
    #     #         context = {'context': data}
    #     #         dataset.append(context)
    #     #         print(context)
    #     #     cnt += 1
    #     # acm_es.bulk_index_data(dataset)
    #     # print('='*150)
    #     # operation.similarity_checking('acm', "data/text.txt")

    #extract information from database. Listformat[year, title, crawlID, PDFPath]
    information=operation.extract_info()
    # file_name=operation.change_file_name('/home/raw_data/pdfs/ACM/0a/0a0a6a2f90b55777da79fbcd2771ac6fa15b0f62','/home/troykuo/target',1)
    # print(file_name)
    # target_path=file_name[:-3]+'txt'
    # print(target_path)
    # with open(r'%s'%file_name, 'rb') as dataIO:
    #     operation.parse(dataIO,target_path)
    info_dic={}
    for info in information:
        # p_year=info[0]
        # p_title=info[1]
        crawl_ID=info[2]
        p_path=info[3]
        #create a dictionary with crawl:[p_year,p_title]
        info_dic[crawl_ID]=info[0:2]
        file_name=operation.change_file_name(p_path,'/home/troykuo/target',crawl_ID)
        print(file_name)
        target_path = file_name[:-3] + 'txt'
        with open(r'%s'%file_name, 'rb') as dataIO:
            operation.parse(dataIO, target_path)

