import json
import os
import time
import MySQLdb
import MySQLdb.cursors
import multiprocessing as mp
import elasticsearch
from tqdm import tqdm_notebook as tqdm
import sys
from functools import partial
import urllib
import importlib
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser, PDFSyntaxError
from pdfminer.pdfdocument import PDFDocument, PDFTextExtractionNotAllowed
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
import paramiko

importlib.reload(sys)


def connect_to_server():
    '''It is used to connect to ACEMAP server'''
    # create SSH obj
    ssh = paramiko.SSHClient()
    # allow to connect the server which is not in know_hosts
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # connect to server
    ssh.connect(hostname='server.acemap.cn', password='', username='', port=10001)
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
    It is used to extract relative information from database RawDataACM,
    including Paper Year, PDFPath and PaperID
    @return these information
    '''
    # database connection
    db = MySQLdb.connect(host="server.acemap.cn", user="readonly", passwd="readonly", port=13306)
    # set cursor for query
    cursor = db.cursor()
    # paperInfo is used to store the data retrieved from database
    paperInfo = []
    sql = """SELECT t.Year, t.PaperID, t.PDFPath FROM crawler_rawdata.RawDataACM t WHERE PDFPath <> '';"""
    cursor.execute(sql)
    for data in cursor:
        print(type(data))
        paperInfo.append(data)
        print(data)
    print(len(paperInfo))
    db.close()
    print(type(paperInfo))
    print(type(paperInfo[2]))
    return paperInfo


def parse(DataIO, save_path):
    '''
    It is used to pare PDF
    @return paper content
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
            paper_context = ""
            for x in layout:
                try:
                    if isinstance(x, LTTextBoxHorizontal):
                        with open('%s' % save_path, 'a') as f:
                            result = x.get_text()
                            print(result)
                            f.write(result + '\n')
                except:
                    print("Failed")

        return paper_context


def create_es_index(host, index, body):
    '''It is used to create new es index'''
    # create es obj and connect to the engine
    es = elasticsearch.Elasticsearch(host)
    print("es.ping(): ", es.ping())
    # create es index
    es.indices.create(index=index, body=body)
    # print("es.indices.get(index) : ",json.dumps(es.indices.get(index), indent=2))


def delete_es_index(host, index):
    '''It is used to delete es index'''
    try:
        es = elasticsearch.Elasticsearch(host)
        es.indices.delete(index)
    except Exception as e:
        print(" option delete error: ", e)


def update_es():
    '''It is used to update index'''
    pass


def change_file_name(source_name):
    '''This is used to change response_body files to pdf'''
    target_name = source_name + '.pdf'
    os.rename(source_name, target_name)
    print("add suffix successfully!")
    print(target_name)
    return target_name


def undo_change_name(source_name):
    '''It is used to change name back'''
    target_name = source_name[:-4]
    os.rename(source_name, target_name)
    print("undo changing name!")


def delete_file(file):
    ''''It is used to delete useless files'''
    os.remove(file)
    print("Delete the file successfully!")
