from bs4 import BeautifulSoup, NavigableString
from utils import timeit, mongo_init, parse_table,generate_unique_id, get_all_books_names, get_file_object_aws, get_toc_from_ncx, get_toc_from_xhtml


bucket_name = 'bud-datalake'
folder_name = 'Books/Oct29-1/'
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"
# print(aws_access_key_id)

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
                
                # Find div with class 'IMAGE'
                image_div = child.find_parent('div')
                parent_div = child.find_parent('div', class_='center-image')
                # Find div with class 'image'
                if parent_div:
                    fig_cap=parent_div.find('p',class_=['FIG_CAPTION', 'FIG_CAPTION_BX'])
                    if fig_cap:
                        img['caption']=fig_cap.get_text(strip=True)
                        print("hello",img['caption'])
                elif image_div:
                    upar_parent=image_div.find_parent('div')
                    if upar_parent:
                        print("yes..")
                        fig_cap=upar_parent.find('p',class_=['FIG_CAPTION', 'FIG_CAPTION_BX'])
                        if fig_cap:
                            img['caption']=fig_cap.get_text(strip=True)
                            print(img['caption'])

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
                previous_p_tag = child.find_previous('p', class_='TAB_CAPTION')
                if previous_p_tag:
                    caption_text = previous_p_tag.get_text(strip=True)
                    print(caption_text)

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
                toc = get_toc_from_ncx(toc_content)
            else:
                toc_content = get_file_object_aws(book, 'toc.xhtml',folder_name, bucket_name)
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
                    file_in_error =files_with_error.find_one(
                        {'book': book, 'filename': filename})
                    if file_in_error:
                        files_with_error.delete_one({'book': book, 'filename': filename})
                    chapter_in_db =oct_chapters.find_one({'book': book, 'filename': filename})
                    if chapter_in_db:
                        if chapter_in_db['sections']:
                            continue
                        elif not chapter_in_db['sections']:
                            oct_chapters.delete_one(
                                {'book': book, 'filename': filename})

                    html_content = get_file_object_aws(book, filename, folder_name, bucket_name)

                    if html_content:
                        try:
                            json_data = parse_html_to_json(
                                html_content, book, filename, db)
                            oct_chapters.insert_one(
                                {'book': book, 'filename': filename, 'sections': json_data, 'order': order_counter})
                            order_counter += 1
                        except Exception as e:
                            print(f'Error while parsing {filename} html >> {e}')
                            files_with_error.insert_one(
                                {'book': book, 'filename': filename, 'error': e})
                            # clear mongo
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
    # to run single book
    # get_book_data('Exam Ref AZ-104 Microsoft Azure Administrator (9780136805328)')
    publisher_collection=db.publishers
    s3_keys=[]
    missing_s3Keys=[]
    extracted=[]
    for book in publisher_collection.find():
        if 'publishers' in book and book['publishers'] and book['publishers'][0].startswith("FT Publishing"):
            if 's3_key' in book:
                bookname=book['s3_key'].split('/')[-2]
                already_extracted=extracted_books.find_one({"book":bookname})
                if not already_extracted:
                    get_book_data(bookname)
                    extracted.append(bookname)
                else:
                    print('already extracted')
                s3_keys.append(bookname)
            else:
                missing_s3Keys.append(book['title'])

    print(f'total books with s3_keys {len(s3_keys)}')
    print(f'total books with out s3_keys {len(missing_s3Keys)}')
    print(f'total ft publication extracted books {len(extracted)}')
