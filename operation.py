import difflib
import os
import MySQLdb
import MySQLdb.cursors
import sys
import importlib
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser, PDFSyntaxError
from pdfminer.pdfdocument import PDFDocument, PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from elasticsearch import Elasticsearch
import paramiko
from shutil import copyfile

importlib.reload(sys)


def connect_to_server():
    '''It is used to connect to Database'''
    # create SSH obj
    ssh = paramiko.SSHClient()
    # allow to connect the server which is not in know_hosts
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # connect to server
    ssh.connect(hostname='server.acemap.cn', password='readonly', username='readonly', port=10001)
    # execute some commands
    command = ''
    stdin, stdout, stderr = ssh.exec_command(command, get_pty=True)
    res, err = stdout.read(), stderr.read()
    print(res)
    stdout.close()
    stdin.close()
    ssh.close()


def extract_info():
    '''
    It is used to extract relative information from database RawDataACM, PDFPath
    including Paper Year, PDFPath and PaperID
    @return these information (year, title, crawlID, PDFPath)
    '''
    # database connection
    db = MySQLdb.connect(host="server.acemap.cn", user="readonly", passwd="readonly", port=13306)
    # set cursor for query
    cursor = db.cursor()
    # paperInfo is used to store the data retrieved from database
    paperInfo = []
    #select year, title, crawlID, PDFPath from 33 forms
    sqls=[
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataAJS t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataAmeghiniana t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataAmetsoc t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataAndgeo t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataBioone t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataDegruyter t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataEGU t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataEpisodes t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataErde t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataErdkunde t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataGeofizika t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataGeoscienceworld t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataIMD t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataIntres t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataJstage t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataMDPI t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataMicropress t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataNature t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataOceanographySociety t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataOfioliti t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataOpenedition t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataOxford t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataPopups t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataSagepub t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataSchweizerbart t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataSEG t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataTandf t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataUB t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataUchicago t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataUnam t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataUnb t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataWiley t WHERE PDFPath <> '';""",
        """SELECT t.Year, t.Title, t.CrawlID, t.PDFPath FROM crawler_rawdata.RawDataZRC t WHERE PDFPath <> '';"""
    ]
    for sql in sqls:
        cursor.execute(sql)
        for data in cursor:
            paperInfo.append(data)
            # print(data)
    print(len(paperInfo))
    db.close()
    return paperInfo


def parse(DataIO, save_path):
    '''
    It is used to pare PDF, and save the content to the target path
    '''
    # create pdf parser
    parser = PDFParser(DataIO)
    # create pdf document
    try:
        doc = PDFDocument(parser)
    except PDFSyntaxError:
        print("can't parse this file!")
        return
    # link document and parser
    parser.set_document(doc)
    # check if the document can be converted to text
    if not doc.is_extractable:
        print("Can't Parse this File! Ignore it and keep parsing")
        raise PDFTextExtractionNotAllowed
    else:
        # create pdf source manager
        rsrcmagr = PDFResourceManager()
        # create PDF device obj
        laparams = LAParams()

        device = PDFPageAggregator(rsrcmagr, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmagr, device)

        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
            # get the LTPage obj, including LTTextBox, LTFigure,
            # LTImage, LTTextBoxHorizontal
            layout = device.get_result()
            for x in layout:
                try:
                    if isinstance(x, LTTextBoxHorizontal):
                        with open('%s' % save_path, 'a') as f:
                            result = x.get_text()
                            f.write(result + '\n')
                except:
                    print("Failed")


def change_file_name(source_path, target_path, crawlID):
    '''This is used to change response_body files to pdf and save'''
    # copy file from source_path to target_path
    source_name = source_path + '/response_body'
    target_name=target_path+'/response_body'
    copyfile(source_name, target_name)
    print("Copy file!")
    # rename file name

    target_name_pdf = target_path + '/%d.pdf' % crawlID
    os.rename(target_name, target_name_pdf)
    print("add suffix successfully!")
    return target_name_pdf


def get_context(file_path):
    context = ""
    try:
        with open(file_path, 'r') as f:
            context = f.read().replace('\n', ' ')
    except FileNotFoundError:
        print("No such file or directory:" + file_path)
        # print(context)
    return context


def undo_change_name(source_name):
    '''It is used to change name back'''
    target_name = source_name[:-4]
    os.rename(source_name, target_name)
    print("undo changing name!")


def delete_file(file):
    ''''It is used to delete useless files'''
    os.remove(file)
    print("Delete the file successfully!")


def es_search(index_name, info, ip='10.10.10.10'):
    '''
    search function for es. Max size=50
    '''
    es = Elasticsearch([ip], port=9200)
    # print("es.ping(): ", es.ping())
    search_body = {
        "query": {
            "match_phrase": {
                "context": {
                    "query": info,
                    # "slop": 0
                }
            }
        }
    }
    result = es.search(index=index_name, size=50, body=search_body)
    similar_text = []
    for hit in result['hits']['hits']:
        # print(hit['_source']['context'])
        similar_text.append(hit['_source']['context'])
    if len(similar_text) == 0:
        # can't find any thing from es
        # print("Nothing matched!")
        return False
    else:
        # match_phrase
        # print("The context is matched!")
        return True


def string_similar(s1, s2):
    return difflib.SequenceMatcher(None, s1, s2).quick_ratio()


def similarity_checking(index_name, file_path):
    context = get_context(file_path)

    if context == '':
        print('something wrong with this file!')
    else:
        # convert from string to list
        context = context.split()
        print(len(context))
        # set initial position, window size (14) and duplicated context number
        current_position = 0
        window_size = 13
        duplicated_context_number = 0
        # sliding window initial
        slide_window = context[current_position:current_position + window_size]

        while len(slide_window) != 0:
            context_str = ' '.join(slide_window)
            match_flag = es_search(index_name, context_str)
            if not match_flag:
                current_position = current_position + 1
                window_size = 13
                slide_window = context[current_position:current_position + window_size]
                continue
            while match_flag:
                # increase window size by one and update slide_window
                window_size += 1
                if current_position + window_size > len(context):
                    window_size = len(context)-current_position+1
                    break
                slide_window = context[current_position:current_position + window_size]
                context_str = ' '.join(slide_window)
                match_flag = es_search(index_name, context_str)
            if window_size>13:
                duplicated_context_number += (window_size -1)
                print("current duplicated words ", duplicated_context_number)
            # print(duplicated_context_number)
            # update slide window
            current_position = current_position + window_size
            window_size = 13
            slide_window = context[current_position:current_position + window_size]
            print(current_position)
        print("duplicated context number", duplicated_context_number)
        print("total number of this paper", len(context))
        duplicated_rate = duplicated_context_number / len(context)
        print("duplicated rate of this document is ", duplicated_rate)
