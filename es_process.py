import operation

if __name__ == '__main__':
    # changed_file = operation.change_file_name('/Users/troykuo/Desktop/acm_papers/response_body')
    # with open(changed_file, 'rb') as pdfIO:
    #     paper_context = operation.parse(pdfIO, '/Users/troykuo/Desktop/acm_papers/response_body.txt')
    # print(paper_context)

    cnt=0
    while cnt <10:
        cnt+=1
        file_path='/Users/troykuo/Desktop/acm_papers/%s.pdf' %cnt
        with open(file_path,'rb') as pdfIO:
            paper_context=operation.parse(pdfIO,'/Users/troykuo/Desktop/acm_papers/%s.txt' %cnt)
        # print(paper_context)
