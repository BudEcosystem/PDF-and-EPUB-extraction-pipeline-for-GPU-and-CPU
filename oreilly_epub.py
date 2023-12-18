import os
import re
import html
from PIL import Image
from urllib.parse import urlparse
import urllib
import boto3
from dotenv import load_dotenv
from latext import latex_to_text
from pymongo import MongoClient
import uuid
import json
import xml.etree.ElementTree as ET
from lxml import etree
from bs4 import BeautifulSoup, NavigableString
from utils import timeit
load_dotenv()


# Configure AWS credentials
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']

mongo_connection_string = os.environ['DATABASE_URL']

# Create an S3 client
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region)

bucket_name = 'bud-datalake'
folder_name = 'Books/Oct29-1/'
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"
# print(aws_access_key_id)

def mongo_init(connection_string=None):
    """
      Initializes and returns a MongoDB database object.

      Returns:
          db (pymongo.database.Database): A MongoDB database object connected to the specified MongoDB cluster.

      Raises:
          pymongo.errors.ConfigurationError: If there is an issue with the MongoDB client configuration.
          pymongo.errors.ConnectionFailure: If a connection to the MongoDB cluster cannot be established.
    """
    db = None
    client = None
    if connection_string is None:
        client = MongoClient()
    else:
        client = MongoClient(connection_string)
    if client:
        db = client['epubstesting']
    return db

@timeit
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text)
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech

def get_aws_s3_contents(bucket_name, folder_name):
    """
      Retrieves the contents of a specific folder in an AWS S3 bucket.

      Args:
          bucket_name (str): The name of the AWS S3 bucket.
          folder_name (str): The name of the folder within the bucket.

      Returns:
          list: A list of dictionaries representing the contents (objects) in the specified folder.

      Raises:
          botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
          botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
          botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
    """
    contents = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name, Prefix=folder_name, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(
                Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' in response:
            contents.extend(response['Contents'])
            print(len(contents))
            print(contents[-1])
        if response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
        else:
            break
    print(len(contents))
    return contents

def download_aws_image(key):
    try:
        os.makedirs(folder_name, exist_ok=True)
        local_path = os.path.join(folder_name, os.path.basename(key))
        s3.download_file(bucket_name, key, local_path)
        return os.path.abspath(local_path)
    except Exception as e:
        print(e)

def get_file_object_aws(book, filename):
    '''
      Get file object from aws s3 bucket

      Args:
          book (str): The name of the book. 
          filename (str): The name of the file.

      Returns:
          str: The contents of the file.

      Raises:
          botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
          botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
          botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
    '''
    file_key = f'{folder_name}{book}/OEBPS/{filename}'
    print(file_key)
    try:
        # Retrieve the object data
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
    except Exception as e:
        return None
    return response['Body'].read().decode('utf-8')

def get_all_books_names(bucket_name, folder_name):
    '''
      Get all books names from aws s3 bucket

      Args:
          bucket_name (str): The name of the AWS S3 bucket.
          folder_name (str): The name of the folder within the bucket.

      Returns:
          list: A list of dictionaries representing the contents (objects) in the specified folder.

      Raises:
          botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
          botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
          botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
    '''
    contents = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=folder_name, Delimiter='/')
    return [each['Prefix'].split('/')[-2] for each in contents['CommonPrefixes']]

def get_book_info(book_content):
    '''
      Get book info from opf file

      Args:
          book_content (str): The contents of the opf file.

      Returns:
          dict: A dictionary representing the book info.

      Raises:
          xml.etree.ElementTree.ParseError: If the provided opf file is invalid.
    '''
    details = {}
    tree = ET.fromstring(book_content)
    # Get the book title
    details['title'] = tree.find(
        ".//{http://purl.org/dc/elements/1.1/}title").text

    # Get the book author
    authors = tree.findall(".//{http://purl.org/dc/elements/1.1/}creator")
    author_names = []
    for each in authors:
        author_names.append(each.text)
    details['authors'] = author_names

    # Get the book publisher
    details['publisher'] = tree.find(
        ".//{http://purl.org/dc/elements/1.1/}publisher").text

    # Get the book publisher
    details['description'] = clean_string(
        tree.find(".//{http://purl.org/dc/elements/1.1/}description").text)

    # Get the book publisher
    details['language'] = tree.find(
        ".//{http://purl.org/dc/elements/1.1/}language").text

    # Get the book publisher
    details['published_date'] = tree.find(
        ".//{http://purl.org/dc/elements/1.1/}date").text

    return details

def get_toc_from_xhtml(toc_contents):
    """
      Extracts the table of contents (TOC) from XHTML content.

      Args:
          toc_contents (str): The XHTML content containing the table of contents.

      Returns:
          list: A list of lists representing the table of contents (TOC). Each inner list contains two elements:
              - label (str): The label or title of a section in the TOC.
              - content (str): The URL or reference to the corresponding content of the section.

      Raises:
          ValueError: If the provided `toc_contents` is not a valid XHTML document.
    """
    soup = BeautifulSoup(toc_contents, 'html.parser')
    toc = []
    for p_tag in soup.find_all('p'):
        a_tags = p_tag.find_all('a')
        for a_tag in a_tags:
            if a_tag.get('href'):
                label = a_tag.text
                content = a_tag['href']
                toc.append([label, content])
    return toc

def get_toc_from_ncx(toc_contents):
    '''
      Get table of content from ncx file

      Args:
          toc_contents (str): The contents of the toc file. 

      Returns:
          list: A list of dictionaries representing the table of content.

      Raises:
          xml.etree.ElementTree.ParseError: If the provided toc file is invalid.
    '''
    parser = etree.XMLParser(recover=True, encoding='utf-8')
    tree = ET.fromstring(toc_contents, parser=parser)
    # root = tree.getroot()
    # Find all navPoint elements
    navpoints = tree.findall(
        ".//{http://www.daisy.org/z3986/2005/ncx/}navPoint")

    # Function to recursively process navPoint elements
    def process_navpoint(navpoint, toc=[]):
        # Extract the label and content attributes
        label = navpoint.find(
            "{http://www.daisy.org/z3986/2005/ncx/}navLabel/{http://www.daisy.org/z3986/2005/ncx/}text").text
        content = navpoint.find(
            "{http://www.daisy.org/z3986/2005/ncx/}content").attrib["src"]

        # Print the label and content
        # print(f"Label: {label}")
        # print(f"Content: {content}")
        toc.append([label, content])

        # Process child navPoint elements recursively
        for child in navpoint.findall("{http://www.daisy.org/z3986/2005/ncx/}navPoint"):
            process_navpoint(child, toc=toc)
        return toc

    # Process each top-level navPoint element
    toc = []
    for navpoint in navpoints:
        toc = process_navpoint(navpoint, toc=toc)
    return toc

def parse_table(table):
    """
      Parses an HTML table and extracts its headers and rows.

      Args:
          table (bs4.element.Tag): The BeautifulSoup Tag object representing the HTML table.

      Returns:
          dict: A dictionary containing the parsed table data.
              - 'headers' (list): A list of strings representing the table headers.
              - 'rows' (list): A list of lists, where each inner list represents a table row and contains
                               strings representing the cell values.

    """
    # Extract the table headers
    headers = [th.get_text(strip=True) for th in table.find_all('th')]

    # Extract the table rows
    rows = []
    for tr in table.find_all('tr'):
        row = [td.get_text(strip=True) for td in tr.find_all('td')]
        if row:
            rows.append(row)
    return {'headers': headers, 'rows': rows}

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
                img = {}
                img['id'] = uuid.uuid4().hex
                aws_path = f'https://{bucket_name}.s3.{aws_region}.amazonaws.com/{folder_name}{book}/OEBPS/'
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
                table_id = uuid.uuid4().hex
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
                equation_Id = uuid.uuid4().hex
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
                code_id = uuid.uuid4().hex
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
    db = mongo_init(mongo_connection_string)
    toc = []
    # check if book exists in db toc collection
    db_toc = db['oct_toc'].find_one({'book': book})
    if db_toc:
        toc = db_toc['toc']
    if not toc:
        error = ''
        try:
            # get table of content
            toc_content = get_file_object_aws(book, 'toc.ncx')
            if toc_content:
                toc = get_toc_from_ncx(toc_content)
            else:
                toc_content = get_file_object_aws(book, 'toc.xhtml')
                if toc_content:
                    toc = get_toc_from_xhtml(toc_content)
        except Exception as e:
            error = str(e)
            print(f'Error while parsing {book} toc >> {e}')
        if not toc:
            db['oct_no_toc'].insert_one({'book': book, 'error': error})
        else:
            db['oct_toc'].insert_one({'book': book, 'toc': toc})

    files = []

    for label, content in toc:
        content_split = content.split('#')
        if len(content_split) > 0:
            filename = content_split[0]
            if filename not in files:
                file_in_error = db['files_with_error'].find_one(
                    {'book': book, 'filename': filename})
                if file_in_error:
                    db['files_with_error'].delete_one(
                        {'book': book, 'filename': filename})
                chapter_in_db = db['oct_chapters'].find_one(
                    {'book': book, 'filename': filename})
                if chapter_in_db:
                    if chapter_in_db['sections']:
                        continue
                    elif not chapter_in_db['sections']:
                        db['oct_chapters'].delete_one(
                            {'book': book, 'filename': filename})
                # print(label, content)
                html_content = get_file_object_aws(book, filename)

                if html_content:
                    try:
                        json_data = parse_html_to_json(
                            html_content, book, filename, db)
                        db['oct_chapters'].insert_one(
                            {'book': book, 'filename': filename, 'sections': json_data})
                    except Exception as e:
                        print(f'Error while parsing {filename} html >> {e}')
                        db['files_with_error'].insert_one(
                            {'book': book, 'filename': filename, 'error': e})
                        # clear mongo
                        db['oct_chapters'].delete_many({'book': book})
                else:
                    print('no html content found : ', filename)
                    db['files_with_error'].insert_one(
                        {'book': book, 'filename': filename, 'error': 'no html content found'})
                files.append(filename)

def check_if_opf_exists(bucket_name, folder_name, book):
    '''
      Check if opf file exists in book folder

      Args:
          book (str): The name of the book.

      Returns:
          files: list of files with .opf extension

      Raises:
          botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
          botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
          botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
    '''
    responses = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}{book}/OEBPS/', ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}{book}/OEBPS/')
        if 'Contents' in response:
            if not responses:
                responses = response['Contents']
            else:
                responses.extend(response['Contents'])
        if 'IsTruncated' in response and response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
        else:
            break
    files = []
    if responses:
        for obj in responses:
            if len(obj['Key'].split('/')) == 4 and obj['Key'].endswith('.opf'):
                files.append(obj['Key'].split('/')[-1])
    return files

def get_heading_tags(elem):
    h_tag = []
    for child in elem.children:
        if child.name:
            if child.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                h_tag.append(child.name)
            elif child.contents:
                h_tag = get_heading_tags(child, h_tag)
    return h_tag

def clean_string(html_string):
    # Unescape HTML entities
    unescaped_string = html.unescape(html_string)
    # Remove HTML tags
    clean_text = re.sub('<[^<]+?>', '', unescaped_string)
    # Strip leading/trailing whitespaces
    clean_text = clean_text.strip()
    return clean_text


db = mongo_init(mongo_connection_string)
publisher_collection=db.publishers
missing_s3_key=db.missing_s3_key_for_oreilly
s3_keys=[]
missing_s3Keys=[]
if __name__ == "__main__":
    for book in publisher_collection.find():
        if 'publishers' in book and book['publishers'] and book['publishers'][0].startswith("O'Reilly"):
            if 's3_key' in book:
                bookname=book['s3_key'].split('/')[-2]
                s3_keys.append(bookname)
                get_book_data(bookname)
            else:
                missing_s3_key.insert_one(book)
                missing_s3Keys.append(book['title'])
    print(f'total books with s3_keys {len(s3_keys)}')
    print(f'total books with s3_keys {len(missing_s3Keys)}')

# get_book_data('Essential Math for Data Science (9781098102920)')



