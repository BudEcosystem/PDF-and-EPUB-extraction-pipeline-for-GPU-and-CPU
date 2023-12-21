from bs4 import BeautifulSoup, NavigableString
from utils import timeit, mongo_init, parse_table, generate_unique_id, get_all_books_names, get_file_object_aws, get_toc_from_ncx, get_toc_from_xhtml
import psutil

bucket_name = 'bud-datalake'
folder_name = 'Books/Oct29-oreilly-2/'
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init('epub_testing')
oct_toc=db.oct_toc
oct_no_toc=db.oct_no_toc
oct_chapters=db.oct_chapters
files_with_error=db.files_with_error
extracted_books=db.extracted_books

def parse_html_to_json(html_content, book, filename):
    # html_content = get_file_object_aws(book, filename)
    soup = BeautifulSoup(html_content, 'html.parser')
    # h_tag = get_heading_tags(soup, h_tag=[])
    section_data = extract_data(
        soup.find('body'), book, filename, section_data=[])
    return section_data

def extract_data(elem, book, filename, section_data=[]):
    for child in elem.children:
        temp = {}
        if isinstance(child, NavigableString):
            if child.strip():
                if section_data:
                    section_data[-1]['content'] += child + ' '
                else:
                    temp['title'] = ''
                    temp['content'] = child + ' '
                    temp['figures'] = []
                    temp['tables'] = []
                    temp['code_snippet'] = []
                    temp['equations'] = []
        elif child.name:
            if child.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                parent_figure = child.find_parent('figure')
                if not parent_figure:
                    if section_data and section_data[-1]['content'].endswith('{{title}} '):
                        section_data[-1]['content'] += child.text.strip() + ' '
                    else:
                        temp['title'] = child.text.strip()
                        temp['content'] = '{{title}}' + ' '
                        temp['figures'] = []
                        temp['tables'] = []
                        temp['code_snippet'] = []
                        temp['equations'] = []

            elif child.name == 'img':
                print("figure here from img")
                img = {}
                img['id'] = generate_unique_id()
                aws_path = f'{s3_base_url}/{folder_name}{book}/OEBPS/'
                img['url'] = aws_path+child['src']
                parent = child.find_parent('figure')
                if parent:
                    h6_tag = parent.find('h6')
                    if h6_tag:
                        img['caption'] = h6_tag.get_text(strip=True)
                if section_data:
                    section_data[-1]['content'] += '{{figure:' + img['id'] + '}} '
                    if 'figures' in section_data[-1]:
                        section_data[-1]['figures'].append(img)
                    else:
                        section_data[-1]['figures'] = [img]

                else:
                    temp['title'] = ''
                    temp['content'] = '{{figure:' + img['id'] + '}} '
                    temp['figures'] = [img]
                    temp['tables'] = []
                    temp['code_snippet'] = []
                    temp['equations'] = []

            elif child.name == 'table':
                print('table here')
                caption_text=''
                caption=child.find('caption')
                if caption:
                    caption_text=caption.get_text(strip=True)
                table_id = generate_unique_id()
                table_data = parse_table(child)
                table = {'id': table_id,
                         'data': table_data, 'caption': caption_text}
                if section_data:
                    section_data[-1]['content'] += '{{table:' + \
                        table['id'] + '}} '
                    if 'tables' in section_data[-1]:
                        section_data[-1]['tables'].append(table)
                        # section_data[-1]['tables'] = [table]
                    else:
                        section_data[-1]['tables'] = [table]
                else:
                    temp['title'] = ''
                    temp['content'] = '{{table:' + table['id'] + '}} '
                    temp['tables'] = [table]
                    temp['figures'] = []
                    temp['code_snippet'] = []
                    temp['equations'] = []
    
            # handling equaiton as text
            elif child.name == 'math':
                print("equation here")
                equation_Id =generate_unique_id()
                eqaution_data = {'id': equation_Id,
                                 'math_tag':str(child)}
                if section_data:
                    section_data[-1]['content'] += '{{equation:' + \
                        equation_Id + '}} '
                    if 'equations' in section_data[-1]:
                        section_data[-1]['equations'].append(eqaution_data)
                    else:
                        section_data[-1]['equations'] = [eqaution_data]
                else:
                    temp['title'] = ''
                    temp['content'] = '{{equation:' + equation_Id + '}} '
                    temp['tables'] = []
                    temp['figures'] = []
                    temp['code_snippet'] = []
                    temp['equations'] = [eqaution_data]

            #code oreilly publication
            elif child.name == 'pre':
                print('code here')
                code_tags = child.find_all('code')
                code = ''
                if code_tags:
                    code = ' '.join(code_tag.get_text(strip=True)
                                    for code_tag in code_tags)
                else:
                    code = child.get_text(strip=True)
                code_id = generate_unique_id()
                code_data = {'id': code_id, 'code_snippet': code}
                if section_data:
                    section_data[-1]['content'] += '{{code_snippet:' + code_id + '}} '
                    if 'code_snippet' in section_data[-1]:
                        section_data[-1]['code_snippet'].append(code_data)

                    else:
                        section_data[-1]['code_snippet'] = [code_data]
                else:
                    temp['title'] = ''
                    temp['content'] = '{{code_snippet:' + code_id + '}} '
                    temp['tables'] = []
                    temp['figures'] = []
                    temp['code_snippet'] = [code_data]
                    temp['equations'] = []
            elif child.contents:
                section_data = extract_data(
                    child, book, filename, section_data=section_data)
        if temp:
            section_data.append(temp)
    return section_data

@timeit
def get_book_data(book):
    print("Book Name >>> ", book)
    toc = []
    # check if book exists in db toc collection
    db_toc = oct_toc.find_one({'book': book})
    if db_toc:
        toc = db_toc['toc']
    if not toc:
        error = ''
        try:
            # get table of content
            toc_content = get_file_object_aws(book, 'toc.ncx', folder_name, bucket_name)
            if toc_content:
                print("hello")
                toc = get_toc_from_ncx(toc_content)
            else:
                print("hello2")
                toc_content = get_file_object_aws(book, 'toc.xhtml', folder_name, bucket_name)
                if toc_content:
                    toc = get_toc_from_xhtml(toc_content)
        except Exception as e:
            error = str(e)
            print(f'Error while parsing {book} toc >> {e}')
        if not toc:
            oct_no_toc.insert_one({'book': book, 'error': error})
        else:
            oct_toc.insert_one({'book': book, 'toc': toc})

    files = []
    order_counter = 0
    prev_filename = None

    for (label, content) in toc:
        content_split = content.split('#')
        if len(content_split) > 0:
            filename = content_split[0]
            if filename != prev_filename:
                if filename not in files:
                    file_in_error = files_with_error.find_one(
                        {'book': book, 'filename': filename})
                    if file_in_error:
                        files_with_error.delete_one(
                            {'book': book, 'filename': filename})
                    chapter_in_db = oct_chapters.find_one(
                        {'book': book, 'filename': filename})
                    if chapter_in_db:
                        if chapter_in_db['sections']:
                            continue
                        elif not chapter_in_db['sections']:
                            oct_chapters.delete_one(
                                {'book': book, 'filename': filename})

                    html_content = get_file_object_aws(book, filename, folder_name, bucket_name)

                    if html_content:
                        try:
                            json_data = parse_html_to_json(html_content, book, filename)
                            oct_chapters.insert_one(
                                {'book': book, 'filename': filename, 'sections': json_data, 'order': order_counter})
                            order_counter += 1
                        except Exception as e:
                            print(f'Error while parsing {filename} html >> {e}')
                            files_with_error.insert_one({'book': book, 'filename': filename, 'error': e})
                            oct_chapters.delete_many({'book': book})
                    else:
                        print('no html content found : ', filename)
                        files_with_error.insert_one(
                            {'book': book, 'filename': filename, 'error': 'no html content found'})
                    files.append(filename)
                prev_filename = filename

    book_data={
        "book":book,
        "extraction":"completed"
    }
    extracted_books.insert_one(book_data)

        
if __name__ == '__main__':
    # run one file
    # get_book_data('Essential Math for Data Science (9781098102920)')
    not_extracted=[]
    #get all books from aws and iterate over it and check if book already extracted
    books = get_all_books_names('bud-datalake', 'Books/Oct29-oreilly-2/')
    print(len(books))
    for book_number, book in enumerate(books, start=0):
        already_extracted=extracted_books.find_one({"book":book})
        mem = psutil.virtual_memory()
        print(f"Total Memory: {mem.total / (1024 ** 3):.2f} GB")
        print(f"Available Memory: {mem.available / (1024 ** 3):.2f} GB")
        print(f"Memory Usage Percentage: {mem.percent:.2f}%")
        if not already_extracted:
            # not_extracted.append(book)
            print(f"Processing book /{book_number} , {book}")
            get_book_data(book)
        else:
            print(f'this {book} already extracted')
    print(len(not_extracted))

# publisher_collection=db.publishers
# s3_keys=[]
# figure_tags=[]
# no_figure_tag=[]
# missing_s3Keys=[]
# extracted=[] 
# for book in publisher_collection.find():
#     if 'publishers' in book and book['publishers'] and book['publishers'][0].startswith("Wiley"):
#         if 's3_key' in book:
#             s3_key=book['s3_key']
#             bookname=book['s3_key'].split('/')[-2]
#             s3_keys.append(bookname)
#             already_extracted=extracted_books.find_one({"book":bookname})
#             if not already_extracted:
#                 print("not_extracted")
#             else:
#                 extracted.append(bookname)
# print(len(extracted))