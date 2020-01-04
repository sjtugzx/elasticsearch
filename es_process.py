import operation

if __name__ == '__main__':
    changed_file = operation.change_file_name('/Users/troykuo/Desktop/acm_papers/response_body')
    with open(changed_file, 'rb') as pdfIO:
        paper_context = operation.parse(pdfIO, '/Users/troykuo/Desktop/acm_papers/response_body.txt')
    print(paper_context)
    operation.undo_change_name(changed_file)
    operation.delete_file('/Users/troykuo/Desktop/acm_papers/response_body.txt')
