import os
import operation
import multiprocessing as mp

from ElasticObj import ElasticObj


def info_Dic(information):
    info_dic = {}
    # response_body-->paperID.pdf-->paperID.txt
    for info in information:
        paper_ID = info[2]
        print(paper_ID)
        p_path = info[3]
        print(p_path)
        # create a dictionary with crawl:[p_year,p_title]
        info_dic[paper_ID] = info[0:2]
    return info_dic


def extract_convert(info):
    paper_ID = info[2]
    print(paper_ID)
    p_path = info[3]
    print(p_path)
    # create a dictionary with crawl:[p_year,p_title]
    file_name = operation.change_file_name(p_path, '/home/troykuo/target', paper_ID)
    target_path = file_name[:-3] + 'txt'
    if (os.path.getsize(file_name) / float(1024 * 1024)) > 60:
        with open('data/largefiles.txt', 'a') as f:
            f.write(paper_ID)
            f.write('\n')
        print("Can't parth such large size file %s." % file_name)
    else:
        if not os.path.exists(target_path):
            with open(r'%s' % file_name, 'rb') as dataIO:
                try:
                    operation.parse(dataIO, target_path,paper_ID)
                    print("parse successfully")
                except:
                    print("Can't parse this file. Because of some problem!!!!!!!!!!!!!")
                    with open('data/parseFailed.txt', 'a') as f:
                        f.write(paper_ID)
                        f.write('\n')


def post_index(info_dic):
    # create es index
    geo_es = ElasticObj('geo')
    geo_es.create_index()
    file_list = os.listdir('/home/troykuo/target')
    dataset = []
    txt_num = 0
    for file in file_list:
        # get context of the txt file
        if file[-4:] == '.txt':
            txt_num += 1
            crawlID = file[:-4]
            title = info_dic[crawlID][1]
            year = info_dic[crawlID][0]
            path = os.path.join('/home/troykuo/target', file)
            data = operation.get_context(path)
            if not (data == ''):
                context = {
                    'title': title,
                    'year': year,
                    'context': data}
                dataset.append(context)

    geo_es.bulk_index_data(dataset)
    print('=' * 150)
    print('the number of txt files is: ', txt_num)


if __name__ == '__main__':


    # extract information from database. Listformat[year, title, crawlID, PDFPath]
    # es=ElasticObj('geo')
    # es.delete_index()
    # post_index(info_dic)
    information = operation.extract_info()
    info_dic=info_Dic(information)

    with mp.Pool(10) as pool:
        pool.map(extract_convert, information)

    # operation.similarity_checking('geo', "data/text.txt")
