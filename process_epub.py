import zipfile
import os
import uuid
from bs4 import NavigableString, BeautifulSoup
import pymongo
from dotenv import load_dotenv
import boto3
import shutil
import xml.etree.ElementTree as ET
load_dotenv()


client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.books
epub_data =db.epub_book
# Configure AWS credentials
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']

# Create an S3 client
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region)

bucket_name = os.environ['AWS_EPUB_BUCKET_NAME']
folder_name = os.environ['BOOK_EPUB_FOLDER_NAME']


def get_all_epub_books_names(bucket_name, folder_name):
    '''
      Get all books names from aws s3 bucket

      Args:
          bucket_name (str): The name of the AWS S3 bucket.
          folder_name (str): The name of the folder within the bucket.

      Returns:
          list: A list of dictionaries representing the contents (objects) in the specified folder.
      '''
    contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    pdf_file_names = [obj['Key'] for obj in contents.get('Contents', [])]
    book_names = [file_name.split('/')[1] for file_name in pdf_file_names]
    return book_names


def download_epub_from_aws(epub):
    '''
      Downloads a epub book from an AWS S3 bucket and saves it as a .zip file to a local directory.

      Args:
          epub(str): The name of the epub book to be downloaded.

      Returns:
          str: The path to the local directory where the EPUB book has been unzipped.
    '''
    local_zip_path = f'{epub}.zip'
    file_key = f'{folder_name}/{epub}'
    s3.download_file(bucket_name, file_key, local_zip_path)
    unzip_folder_path = f'{epub}_unzipped'
    os.makedirs(unzip_folder_path, exist_ok=True)
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_folder_path)
    os.remove(local_zip_path)
    return unzip_folder_path


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


def extract_data(elem, section_data=[], base_path=''):
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
        elif child.name:
            if child.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                if section_data and section_data[-1]['content'].endswith('{{title}} '):
                    section_data[-1]['content'] += child.text.strip() + ' '
                else:
                    temp['title'] = child.text.strip()
                    temp['content'] = '{{title}}' + ' '
                    temp['figures'] = []
                    temp['tables'] = []
                    temp['code_snippet'] = []
            elif (child.name == 'img' and
                  child.parent and
                  ('mediaobject' in child.parent.get('class', []) or child.parent.get('id') == 'cover-image')):
                img = {}
                img['id'] = uuid.uuid4().hex
                image_path = base_path.replace("\\", "/")+'/'+child['src']
                image_url = upload_to_aws_s3(image_path, img['id'])
                img['url'] = image_url
                caption_element = child.find_next('div', class_='caption')
                img['caption'] = caption_element.text.strip(
                ) if caption_element else ''
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
            elif child.name == 'table':
                caption = child.find('caption')
                caption = caption.text.strip() if caption else ''
                table_id = uuid.uuid4().hex
                table_data = parse_table(child)
                table = {'id': table_id,
                         'data': table_data, 'caption': caption}
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
            elif child.contents:
                section_data = extract_data(
                    child, section_data=section_data, base_path=base_path)
        if temp:
            section_data.append(temp)
    return section_data


def get_toc_from_ncx(toc_contents):
    '''
      Get table of content from ncx files

      Args:
          toc_contents (str): The contents of the toc file.

      Returns:
          list: A list of string representing the table of content.

    '''
    root = ET.fromstring(toc_contents)
    toc_array = []
    for content_element in root.findall(".//{http://www.daisy.org/z3986/2005/ncx/}content"):
        src = content_element.get("src")
        toc_array.append(src)

    return toc_array


def process_epub_book(epub):
    '''
    Process an EPUB book after downloading from AWS S3.

    Args:
        epub (str): The name of the EPUB book to be processed.

    Returns:
          None

    This function downloads an EPUB book from AWS S3, extracts the table of contents (TOC) from
    its 'toc.ncx' file, and processes the content of the book. The processed data is stored in a
    database. Finally, the downloaded EPUB is removed.

    '''
    unzip_folder_path = download_epub_from_aws(epub)
    relative_oebps_path = 'OEBPS'

    oebps_folder_path = os.path.abspath(
        os.path.join(unzip_folder_path, relative_oebps_path))

    toc_ncx_path = os.path.abspath(os.path.join(
        unzip_folder_path, relative_oebps_path, 'toc.ncx'))

    with open(toc_ncx_path, 'rb') as file:
        toc_contents = file.read()

    toc_contents = get_toc_from_ncx(toc_contents)

    toc_contents = [
        file_name for file_name in toc_contents if file_name != 'index.xhtml']

    if os.path.exists(oebps_folder_path) and os.path.isdir(oebps_folder_path):
        for file_name in toc_contents:
            file_path = os.path.join(oebps_folder_path, file_name)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as file:
                    html_content = file.read()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    section_data = extract_data(
                        soup.body, section_data=[], base_path=oebps_folder_path)
                    fileName = os.path.basename(file_path)
                    document = {
                        'book': epub,
                        'fileName': fileName,
                        'sections': section_data
                    }
                    epub_data.insert_one(document)

    shutil.rmtree(unzip_folder_path)


def upload_to_aws_s3(figure_image_path, figureId):
    """
    Uploads an image to an Amazon S3 bucket and returns its URL.

    Args:
    figure_image_path (str): The local file path to the image to be uploaded.
    figureId (str): A unique identifier for the image.

    Returns:
    str: The URL of the uploaded image in the Amazon S3 bucket.
    """
    folderName = os.environ['AWS_EPUB_IMAGE_UPLOAD_FOLDER']
    s3_key = f"{folderName}/{figureId}.png"
    # Upload the image to the specified S3 bucket
    s3.upload_file(figure_image_path, bucket_name, s3_key)
    # Get the URL of the uploaded image
    figure_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"

    return figure_url


all_epub_books = get_all_epub_books_names(bucket_name, folder_name + '/')
print(len(all_epub_books))

for book_number, epub in enumerate(all_epub_books, start=1):
    if epub.endswith('.epub'):
        print(f"Processing Book {book_number}: {epub}")
        process_epub_book(epub)


# #Process single epub book
# process_epub_book("asimov-genetic-effects-of-radiation.epub")
