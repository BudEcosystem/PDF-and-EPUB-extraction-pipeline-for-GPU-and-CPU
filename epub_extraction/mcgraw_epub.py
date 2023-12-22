import os
import pytesseract
from bs4 import BeautifulSoup, NavigableString
import shutil
from PIL import Image
from extract_epub_table import process_book_page
from utils import timeit, mongo_init,generate_unique_id, get_s3, parse_table,get_file_object_aws,get_toc_from_xhtml, get_all_books_names, get_toc_from_ncx


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



def download_aws_image(key, book):
    try:
        if os.path.exists(book):
            shutil.rmtree(book)
        os.makedirs(book)
        local_path = os.path.join(book, os.path.basename(key))
        s3 = get_s3()
        s3.download_file(bucket_name, key, local_path)
        return os.path.abspath(local_path)
    except Exception as e:
        print(e)
        return None

def get_figure_caption(parent):
    figure_caption=''
    prev_sib=parent.find_previous_sibling()
    next_sib=parent.find_next_sibling()
    if prev_sib:
        classname=prev_sib.get('class', [''])[0]
        if classname=='figcap':
            figure_caption=prev_sib.get_text(strip=True)
        elif next_sib:
            classname=next_sib.get('class', [''])[0]
            if classname=='figcap':
                figure_caption=next_sib.get_text(strip=True)
    return figure_caption


def parse_html_to_json(html_content, book, filename, db):
    # html_content = get_file_object_aws(book, filename)
    soup = BeautifulSoup(html_content, 'html.parser')
    # h_tag = get_heading_tags(soup, h_tag=[])
    section_data = extract_data(
        soup.find('body'), book, filename, db, section_data=[])
    return section_data

def extract_data(elem, book, filename, db, section_data=[]):
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
                figure_caption=''
                id = generate_unique_id()
                aws_path = f'{s3_base_url}/{folder_name}{book}/OEBPS/'
                url= aws_path+child['src']
                
                #if image are inside p tag with class name f-image
                image_parent=child.find_parent('p', class_='fimage')
                #if image are inside p tag with class name figimgc
                figimgc_parent= child.find_parent('p', class_="figimgc")
                #if image are inside p tag with class name images
                images_parent=child.find_parent('p', class_='images')
                #if image are inside p tag with class name f-image
                imagec_parent=child.find_parent('p', class_='imagec')
            
                if image_parent:
                   figure_caption=get_figure_caption(image_parent)

                elif figimgc_parent: 
                    prev_sib=figimgc_parent.find_previous_sibling()
                    next_sib=figimgc_parent.find_next_sibling()
                    if prev_sib:
                       classname=prev_sib.get('class', [''])[0]
                       if classname=='figcapl':
                           figure_caption=prev_sib.get_text(strip=True)
                       elif next_sib:
                           classname=next_sib.get('class', [''])[0]
                           if classname=='figcapl':
                               figure_caption=next_sib.get_text(strip=True)

                elif images_parent:
                    figure_caption=get_figure_caption(images_parent)

                elif imagec_parent:
                    figure_caption=get_figure_caption(imagec_parent)
                                        
                img={'id':id, 'url':url, 'caption':figure_caption}
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
            
            #handling p tag with class name image, it contains either image or table image
            elif child.name == 'p' and 'image' in child.get('class', []):
                print("inside p with classname image")
                image_tag=child.find('img')
                if not image_tag:
                    continue
                id = generate_unique_id()
                aws_path = f'{s3_base_url}/{folder_name}{book}/OEBPS/'
                image_path = aws_path+image_tag['src']
                figure_caption=''
                table_caption=''
                fig_next_sib_class=''
                fig_prev_sib_class=''
                # we are doing like this beacuase we find immediate next sibling of image class
                next_sibling = child.find_next_sibling()
                previous_sibling=child.find_previous_sibling()
                if next_sibling:
                    fig_next_sib_class=next_sibling.get('class', [''])[0]
                if previous_sibling:
                    fig_prev_sib_class=previous_sibling.get('class',[''])[0]
                if fig_next_sib_class=='figcap':
                    figure_caption=next_sibling.get_text(strip=True)
                elif fig_next_sib_class=='tabcap':
                    table_caption=next_sibling.get_text(strip=True)
                elif fig_prev_sib_class=='figcap':
                    figure_caption=previous_sibling.get_text(strip=True)
                if table_caption!="":
                    img_key = image_path.replace(s3_base_url + "/", "")
                    table_image_path = download_aws_image(img_key, book)
                    print(table_image_path)
                    if not table_image_path:
                        continue
                    try:
                        data=process_book_page(table_image_path)
                    except Exception as e:
                        print("error while extrcating table using bud-ocr")
                        continue
                    table={"id":id, "data":data, "caption":table_caption}   
                    if os.path.exists(table_image_path):
                        os.remove(table_image_path)
                    if section_data:
                        section_data[-1]['content'] += '{{table:' + table['id'] + '}} '
                        if 'tables' in section_data[-1]:
                            section_data[-1]['tables'].append(table)
                        else:
                            section_data[-1]['tables'] = [table]
                    else:
                        temp['title'] = ''
                        temp['content'] = '{{table:' + table['id'] + '}} '
                        temp['tables'] = [table]
                        temp['figures'] = []
                        temp['code_snippet'] = []
                        temp['equations'] = []
                else:
                    img={'id':id,'url':image_path,'caption':figure_caption}
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


            # handle table as image p tag with class name timage and image-t
            elif child.name == 'p' and any(cls in child.get('class', []) for cls in ['timage', 'image-t', 'imaget']):
                print("hello")
                caption=''
                next_sibling = child.find_next_sibling()
                previous_sibling=child.find_previous_sibling()
                if next_sibling:
                    tab_next_sib_class=next_sibling.get('class', [''])[0]
                    if tab_next_sib_class=='tabcap':
                        caption=next_sibling.get_text(strip=True)
                    elif previous_sibling:
                        tab_prev_sib_class=previous_sibling.get('class',[''])[0]
                        if tab_prev_sib_class=='tabcap':
                            caption=previous_sibling.get_text(strip=True)

                print("yes table found and its caption is", caption)
                table_image = child.find('img')
                table_id = generate_unique_id()
                if table_image:
                    aws_path = f'{s3_base_url}/{folder_name}{book}/OEBPS/'
                    img_url = aws_path + table_image['src']
                    print("This is table image")
                    img_key = img_url.replace(s3_base_url + "/", "")
                    table_image_path = download_aws_image(img_key, book)
                    print(table_image_path)
                    if not table_image_path:
                        continue
                    try:
                        data=process_book_page(table_image_path)
                    except Exception as e:
                        print("error while extrcating table using bud-ocr",e)
                        continue
                    table={"id":table_id, "data":data, "caption":caption} 
                    if os.path.exists(table_image_path):
                        os.remove(table_image_path)
                if section_data:
                    section_data[-1]['content'] += '{{table:' + \
                        table['id'] + '}} '
                    if 'tables' in section_data[-1]:
                        section_data[-1]['tables'].append(table)
                    else:
                        section_data[-1]['tables'] = [table]
                else:
                    temp['title'] = ''
                    temp['content'] = '{{table:' + table['id'] + '}} '
                    temp['tables'] = [table]
                    temp['figures'] = []
                    temp['code_snippet'] = []
                    temp['equations'] = []
            
            #handle image and code
            elif child.name == 'p' and 'code' in child.get('class', []):
                code_tag = child.find('code')
                image_tag = child.find('img')
                if code_tag:
                    print("code inside code")
                    code = code_tag.get_text(strip=True)
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

                elif image_tag:
                    print("image inside class code")
                    img = {}
                    img['id'] = generate_unique_id()
                    aws_path = f'{s3_base_url}/{folder_name}{book}/OEBPS/'
                    img['url'] = aws_path+image_tag['src']
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
                
            #code inside p tag with class name code1
            elif child.name == 'p' and any(cls in child.get('class', []) for cls in ['code1', 'imagepre', 'imageprei']):
                print('code here')
                code_tag = child.find('code')
                code_image=child.find('img')
                code = ''
                if code_tag:
                    code = code_tag.get_text(strip=True)
                elif code_image:
                    aws_path = f'{s3_base_url}/{folder_name}{book}/OEBPS/'
                    img_url = aws_path + code_image['src']
                    print("This is code image")
                    img_key = img_url.replace(s3_base_url + "/", "")
                    code_image_path= download_aws_image(img_key,book)
                    if not code_image_path:
                        continue
                    try:
                        image =Image.open(code_image_path)
                    except Exception as e:
                        print("error while reading code image",e)
                        continue
                    code = pytesseract.image_to_string(image)
                    if os.path.exists(code_image_path):
                        os.remove(code_image_path)
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

            elif child.name == 'table':
                print('table here')
                caption_text=''
                parent = child.find_parent('figure',class_='table')
                # Find div with class 'table-contents'
                table_contents_div = child.find_parent('div', class_='table-contents')
                if parent:
                    tabcaption = parent.find('figcaption')
                    if tabcaption:
                        caption_text = tabcaption.get_text(strip=True)
                elif table_contents_div:
                    table_div = table_contents_div.find_parent('div', class_='table')
                    if table_div:
                        table_title=table_div.find('div',class_='table-title')
                        table_title2=table_div.find('p',class_='title')
                        if table_title:
                            caption_text=table_title.get_text(strip=True)
                        elif table_title2:
                            caption_text=table_title2.get_text(strip=True)

                table_id =generate_unique_id()
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
                    child, book, filename, db, section_data=section_data)
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
    #run single book
    # get_book_data('OCA Java SE 8 Programmer I Exam Guide (Exams 1Z0-808) (9781260011388)')
    publisher_collection=db.publishers
    s3_keys=[]
    missing_s3Keys=[]
    extracted=[]
    for book in publisher_collection.find():
        if 'publishers' in book and book['publishers'] and book['publishers'][0].startswith("Mc"):
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
    print(f'total mcgraw extracted books {len(extracted)}')
