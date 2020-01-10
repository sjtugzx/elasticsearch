import os

import operation
from ElasticObj import ElasticObj

if __name__ == '__main__':
    # #extract information from database. Listformat[year, title, crawlID, PDFPath]
    # information=operation.extract_info()
    #
    # info_dic={}
    # #response_body-->crawlID.pdf-->crawlID.txt
    # for info in information:
    #     # p_year=info[0]
    #     # p_title=info[1]
    #     crawl_ID=info[2]
    #     p_path=info[3]
    #     #create a dictionary with crawl:[p_year,p_title]
    #     info_dic[crawl_ID]=info[0:2]
    #     file_name=operation.change_file_name(p_path,'/home/troykuo/target',crawl_ID)
    #     print(file_name)
    #     target_path = file_name[:-3] + 'txt'
    #     with open(r'%s'%file_name, 'rb') as dataIO:
    #         operation.parse(dataIO, target_path)
    #
    # #create es index
    # geo_es = ElasticObj('geo')
    # geo_es.create_index()
    file_list=os.listdir('/home/troykuo/target')
    dataset=[]
    for file in file_list:
        if file[-4:]=='.txt':
            print(file)
        # data=operation.get_context(txt)
        # if not (data==''):
        #     context={'context':data}
        #     dataset.append(context)

    # geo_es.bulk_index_data(dataset)
    print('='*150)

    #     # operation.similarity_checking('acm', "data/text.txt")

