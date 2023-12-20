# import os
# import re
# import html
# from PIL import Image
# from urllib.parse import urlparse
# import urllib
# from latext import latex_to_text
# import boto3
# from botocore.exceptions import ClientError
# from pymongo import MongoClient
# import uuid
# from latext import latex_to_text
# from pix2tex.cli import LatexOCR
# import json
# import xml.etree.ElementTree as ET
# from lxml import etree
# from bs4 import BeautifulSoup, NavigableString
# from utils import timeit

# # Configure AWS credentials
# aws_access_key_id = ''
# aws_secret_access_key = ''
# aws_region = ''

# mongo_connection_string = ""

# # Create an S3 client
# s3 = boto3.client('s3',
#                   aws_access_key_id=aws_access_key_id,
#                   aws_secret_access_key=aws_secret_access_key,
#                   region_name=aws_region)

# bucket_name = 'bud-datalake'
# folder_name = 'Books/Oct29-1/'
# s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"
# # print(aws_access_key_id)

# latex_ocr=LatexOCR()

# def mongo_init(connection_string=None):
#     """
#       Initializes and returns a MongoDB database object.

#       Returns:
#           db (pymongo.database.Database): A MongoDB database object connected to the specified MongoDB cluster.

#       Raises:
#           pymongo.errors.ConfigurationError: If there is an issue with the MongoDB client configuration.
#           pymongo.errors.ConnectionFailure: If a connection to the MongoDB cluster cannot be established.
#     """
#     db = None
#     client = None
#     if connection_string is None:
#         client = MongoClient()
#     else:
#         client = MongoClient(connection_string)
#     if client:
#         db = client['epubstesting']
#     return db


# def get_aws_s3_contents(bucket_name, folder_name):
#     """
#       Retrieves the contents of a specific folder in an AWS S3 bucket.

#       Args:
#           bucket_name (str): The name of the AWS S3 bucket.
#           folder_name (str): The name of the folder within the bucket.

#       Returns:
#           list: A list of dictionaries representing the contents (objects) in the specified folder.

#       Raises:
#           botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
#           botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
#           botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
#     """
#     contents = []
#     continuation_token = None
#     while True:
#         if continuation_token:
#             response = s3.list_objects_v2(
#                 Bucket=bucket_name, Prefix=folder_name, ContinuationToken=continuation_token)
#         else:
#             response = s3.list_objects_v2(
#                 Bucket=bucket_name, Prefix=folder_name)
#         if 'Contents' in response:
#             contents.extend(response['Contents'])
#             print(len(contents))
#             print(contents[-1])
#         if response['IsTruncated']:
#             continuation_token = response['NextContinuationToken']
#         else:
#             break
#     print(len(contents))
#     return contents


# def download_aws_image(key):
#     try:
#         os.makedirs(folder_name, exist_ok=True)
#         local_path = os.path.join(folder_name, os.path.basename(key))
#         s3.download_file(bucket_name, key, local_path)
#         return os.path.abspath(local_path)
#     except Exception as e:
#         print(e)


# def get_file_object_aws(book, filename):
#     '''
#       Get file object from aws s3 bucket

#       Args:
#           book (str): The name of the book. 
#           filename (str): The name of the file.

#       Returns:
#           str: The contents of the file.

#       Raises:
#           botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
#           botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
#           botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
#     '''
#     file_key = f'{folder_name}{book}/OEBPS/{filename}'
#     print(file_key)
#     try:
#         # Retrieve the object data
#         response = s3.get_object(Bucket=bucket_name, Key=file_key)
#     except Exception as e:
#         return None
#     return response['Body'].read().decode('utf-8')


# def get_all_books_names(bucket_name, folder_name):
#     '''
#       Get all books names from aws s3 bucket

#       Args:
#           bucket_name (str): The name of the AWS S3 bucket.
#           folder_name (str): The name of the folder within the bucket.

#       Returns:
#           list: A list of dictionaries representing the contents (objects) in the specified folder.

#       Raises:
#           botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
#           botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
#           botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
#     '''
#     contents = s3.list_objects_v2(
#         Bucket=bucket_name, Prefix=folder_name, Delimiter='/')
#     return [each['Prefix'].split('/')[-2] for each in contents['CommonPrefixes']]


# def get_book_info(book_content):
#     '''
#       Get book info from opf file

#       Args:
#           book_content (str): The contents of the opf file.

#       Returns:
#           dict: A dictionary representing the book info.

#       Raises:
#           xml.etree.ElementTree.ParseError: If the provided opf file is invalid.
#     '''
#     details = {}
#     tree = ET.fromstring(book_content)
#     # Get the book title
#     details['title'] = tree.find(
#         ".//{http://purl.org/dc/elements/1.1/}title").text

#     # Get the book author
#     authors = tree.findall(".//{http://purl.org/dc/elements/1.1/}creator")
#     author_names = []
#     for each in authors:
#         author_names.append(each.text)
#     details['authors'] = author_names

#     # Get the book publisher
#     details['publisher'] = tree.find(
#         ".//{http://purl.org/dc/elements/1.1/}publisher").text

#     # Get the book publisher
#     details['description'] = clean_string(
#         tree.find(".//{http://purl.org/dc/elements/1.1/}description").text)

#     # Get the book publisher
#     details['language'] = tree.find(
#         ".//{http://purl.org/dc/elements/1.1/}language").text

#     # Get the book publisher
#     details['published_date'] = tree.find(
#         ".//{http://purl.org/dc/elements/1.1/}date").text

#     return details


# def get_toc_from_xhtml(toc_contents):
#     """
#       Extracts the table of contents (TOC) from XHTML content.

#       Args:
#           toc_contents (str): The XHTML content containing the table of contents.

#       Returns:
#           list: A list of lists representing the table of contents (TOC). Each inner list contains two elements:
#               - label (str): The label or title of a section in the TOC.
#               - content (str): The URL or reference to the corresponding content of the section.

#       Raises:
#           ValueError: If the provided `toc_contents` is not a valid XHTML document.
#     """
#     soup = BeautifulSoup(toc_contents, 'html.parser')
#     toc = []
#     for p_tag in soup.find_all('p'):
#         a_tags = p_tag.find_all('a')
#         for a_tag in a_tags:
#             if a_tag.get('href'):
#                 label = a_tag.text
#                 content = a_tag['href']
#                 toc.append([label, content])
#     return toc


# def get_toc_from_ncx(toc_contents):
#     '''
#       Get table of content from ncx file

#       Args:
#           toc_contents (str): The contents of the toc file. 

#       Returns:
#           list: A list of dictionaries representing the table of content.

#       Raises:
#           xml.etree.ElementTree.ParseError: If the provided toc file is invalid.
#     '''
#     parser = etree.XMLParser(recover=True, encoding='utf-8')
#     tree = ET.fromstring(toc_contents, parser=parser)
#     # root = tree.getroot()
#     # Find all navPoint elements
#     navpoints = tree.findall(
#         ".//{http://www.daisy.org/z3986/2005/ncx/}navPoint")

#     # Function to recursively process navPoint elements
#     def process_navpoint(navpoint, toc=[]):
#         # Extract the label and content attributes
#         label = navpoint.find(
#             "{http://www.daisy.org/z3986/2005/ncx/}navLabel/{http://www.daisy.org/z3986/2005/ncx/}text").text
#         content = navpoint.find(
#             "{http://www.daisy.org/z3986/2005/ncx/}content").attrib["src"]

#         # Print the label and content
#         # print(f"Label: {label}")
#         # print(f"Content: {content}")
#         toc.append([label, content])

#         # Process child navPoint elements recursively
#         for child in navpoint.findall("{http://www.daisy.org/z3986/2005/ncx/}navPoint"):
#             process_navpoint(child, toc=toc)
#         return toc

#     # Process each top-level navPoint element
#     toc = []
#     for navpoint in navpoints:
#         toc = process_navpoint(navpoint, toc=toc)
#     return toc


# def parse_table(table):
#     """
#       Parses an HTML table and extracts its headers and rows.

#       Args:
#           table (bs4.element.Tag): The BeautifulSoup Tag object representing the HTML table.

#       Returns:
#           dict: A dictionary containing the parsed table data.
#               - 'headers' (list): A list of strings representing the table headers.
#               - 'rows' (list): A list of lists, where each inner list represents a table row and contains
#                                strings representing the cell values.

#     """
#     # Extract the table headers
#     headers = [th.get_text(strip=True) for th in table.find_all('th')]

#     # Extract the table rows
#     rows = []
#     for tr in table.find_all('tr'):
#         row = [td.get_text(strip=True) for td in tr.find_all('td')]
#         if row:
#             rows.append(row)
#     return {'headers': headers, 'rows': rows}


# def parse_html_to_json(html_content, book, filename, db):
#     # html_content = get_file_object_aws(book, filename)
#     soup = BeautifulSoup(html_content, 'html.parser')
#     # h_tag = get_heading_tags(soup, h_tag=[])
#     section_data = extract_data(
#         soup.find('body'), book, filename, db, section_data=[])
#     return section_data


# @timeit
# def latext_to_text_to_speech(text):
#     # Remove leading backslashes and add dollar signs at the beginning and end of the text
#     text = "${}$".format(text.lstrip('\\'))
#     # Convert the LaTeX text to text to speech
#     text_to_speech = latex_to_text(text)
#     return text_to_speech


# def extract_data(elem, book, filename, db, section_data=[]):
#     for child in elem.children:
#         temp = {}
#         if isinstance(child, NavigableString):
#             if child.strip():
#                 if section_data:
#                     section_data[-1]['content'] += child + ' '
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = child + ' '
#                     temp['figures'] = []
#                     temp['tables'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = []
#         elif child.name:
#             if child.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
#                 parent_figure = child.find_parent('figure')
#                 if not parent_figure:
#                     if section_data and section_data[-1]['content'].endswith('{{title}} '):
#                         section_data[-1]['content'] += child.text.strip() + ' '
#                     else:
#                         temp['title'] = child.text.strip()
#                         temp['content'] = '{{title}}' + ' '
#                         temp['figures'] = []
#                         temp['tables'] = []
#                         temp['code_snippet'] = []
#                         temp['equations'] = []

#             elif child.name == 'img':
#                 print("figure here from img")
#                 img = {}
#                 img['id'] = uuid.uuid4().hex
#                 parent2 = child.find_parent('div')
#                 print(parent2.get('class', []))

#                 #aws_path = f'https://{bucket_name}.s3.{
#                 #aws_region}.amazonaws.com/{folder_name}{book}/OEBPS/'
#                 #img['url'] = aws_path+child['src']
#                 img['caption'] = ''
#                 caption_element = child.find_previous_sibling(
#                     'p', class_='caption')
#                 caption_element2 = child.find_previous_sibling(
#                     'h5', class_='notetitle')
#                 p_parent = child.find_parent('p', class_=re.compile(
#                     'fimage|fm-figure', re.I))
#                 if caption_element:
#                     img['caption'] = caption_element.get_text(strip=True)
#                 elif caption_element2:
#                     img['caption'] = caption_element.get_text(strip=True)
#                 elif p_parent:
#                     figcap = p_parent.find_next_sibling(
#                         'p', class_=re.compile('figcap|fm-figure-caption', re.I))
#                     img['caption'] = figcap.get_text(
#                         strip=True) if figcap else ''
#                 parent = child.find_parent('figure')
#                 imagewrap_parent = child.find_parent('div', class_='imagewrap')
#                 if imagewrap_parent:
#                     p = imagewrap_parent.find('p')
#                     if p:
#                         img['caption'] = p.get_text(strip=True)
#                 elif parent:
#                     figcaption_tag = parent.find('figcaption')
#                     h6_tag = parent.find('h6')
#                     captag1 = parent.find('p', class_='figurecaption')
#                     if figcaption_tag:
#                         img['caption'] = figcaption_tag.get_text(strip=True)
#                     elif h6_tag:
#                         img['caption'] = h6_tag.get_text(strip=True)
#                     elif captag1:
#                         img['caption'] = captag1.get_text(strip=True)
#                 else:
#                     sibling_paragraph = child.find_next(
#                         'p', class_='figcaption')
#                     caption_p = child.find_parent('p', class_='center')
#                     if sibling_paragraph:
#                         img['caption'] = sibling_paragraph.get_text(strip=True)
#                     if caption_p:
#                         next_p = caption_p.find_next_sibling(
#                             'p', class_='caption')
#                         if next_p and 'caption' in next_p.get('class', []):
#                             img['caption'] = next_p.get_text(strip=True)

#                 if section_data:
#                     section_data[-1]['content'] += '{{figure:' + img['id'] + '}} '
#                     if 'figures' in section_data[-1]:
#                         section_data[-1]['figures'].append(img)
#                     else:
#                         section_data[-1]['figures'] = [img]

#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{figure:' + img['id'] + '}} '
#                     temp['figures'] = [img]
#                     temp['tables'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = []
#                 if not img['caption']:
#                     existing_document = db['image_with_no_caption'].find_one(
#                         {'book': book, 'filename': filename})
#                     if existing_document:
#                         db['image_with_no_caption'].update_one(
#                             {'book': book, 'filename': filename}, {'$push': {'images': img}})
#                     else:
#                         db['image_with_no_caption'].insert_one(
#                             {'book': book, 'filename': filename, 'images': [img]})

#             elif child.name == 'table':
#                 print("table here")
#                 caption = ''
#                 caption_element = child.find_previous_sibling(
#                     'p', class_='tabcaption')
#                 caption = caption_element.text.strip() if caption_element else ''
#                 # Look for the parent div with class 'Table'
#                 parent_div = child.find_parent(
#                     'div', class_=re.compile('Table|group|table-contents', re.I))
#                 if parent_div:
#                     second_parent = parent_div.find_parent('table')
#                     if second_parent:
#                         tabcap = second_parent.find('p', class_='title')
#                         caption = tabcap.get_text(strip=True) if tabcap else ''
#                     # Look for a div with class 'Caption' within the parent div
#                     caption_div = parent_div.find('div', class_='Caption')
#                     captag1 = parent_div.find('p')
#                     caption_element = parent_div.find_previous_sibling(
#                         'p', class_='title')
#                     if caption_div:
#                         # Look for 'captionContent' div within 'Caption' div
#                         caption_content_div = caption_div.find(
#                             'div', class_='CaptionContent')

#                         # Extract text from both the <span> and <p> tags within 'captionContent' div
#                         if caption_content_div:
#                             span_element = caption_content_div.find('span')
#                             span_text = span_element.text.strip() if span_element else ''
#                             p_element = caption_content_div.find('p')
#                             p_text = p_element.text.strip() if p_element else ''

#                             # Combine span and p text if both are present
#                             caption = f"{span_text} {p_text}".strip()
#                     elif captag1:
#                         caption = captag1.get_text(strip=True)
#                     elif caption_element:
#                         caption = caption_element.text.strip() if caption_element else ''

#                 table_id = uuid.uuid4().hex
#                 table_data = parse_table(child)
#                 table = {'id': table_id,
#                          'data': table_data, 'caption': caption}
#                 if section_data:
#                     section_data[-1]['content'] += '{{table:' + \
#                         table['id'] + '}} '
#                     if 'tables' in section_data[-1]:
#                         section_data[-1]['tables'].append(table)
#                         # section_data[-1]['tables'] = [table]
#                     else:
#                         section_data[-1]['tables'] = [table]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{table:' + table['id'] + '}} '
#                     temp['tables'] = [table]
#                     temp['figures'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = []
#                 if not caption:
#                     existing_document = db['table_with_no_caption'].find_one(
#                         {'book': book, 'filename': filename})
#                     if existing_document:
#                         db['table_with_no_caption'].update_one(
#                             {'book': book, 'filename': filename}, {'$push': {'tables': table}})
#                     else:
#                         db['table_with_no_caption'].insert_one(
#                             {'book': book, 'filename': filename, 'tables': [table]})

#             elif child.name == 'p' and 'center1' in child.get('class', []) and not child.find('img'):
#                 equation_Id = uuid.uuid4().hex
#                 equation_text = child.get_text(strip=True)
#                 if equation_text.startswith('Figure'):
#                     # Skip processing if it starts with "Figure"
#                     continue
#                 eqaution_data = {'id': equation_Id,
#                                  'text': equation_text}
#                 if section_data:
#                     section_data[-1]['content'] += '{{equation:' + \
#                         equation_Id + '}} '
#                     if 'equations' in section_data[-1]:
#                         section_data[-1]['equations'].append(eqaution_data)
#                     else:
#                         section_data[-1]['equations'] = [eqaution_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{equation:' + equation_Id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = [eqaution_data]

#             # equation as text
#             elif child.name == 'div' and 'Kindlecenter' in child.get('class', []):
#                 equation_Id = uuid.uuid4().hex
#                 equation_text = child.get_text(strip=True)
#                 eqaution_data = {'id': equation_Id,
#                                  'text': equation_text}
#                 if section_data:
#                     section_data[-1]['content'] += '{{equation:' + \
#                         equation_Id + '}} '
#                     if 'equations' in section_data[-1]:
#                         section_data[-1]['equations'].append(eqaution_data)
#                     else:
#                         section_data[-1]['equations'] = [eqaution_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{equation:' + equation_Id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = [eqaution_data]

#              # 100%  equation image
           
#             elif child.name == 'div' and ('equationNumbered' in child.get('class', []) or 'informalEquation' in child.get('class', [])):
#                 equation_image = child.find('img')
#                 equation_Id = uuid.uuid4().hex
#                 if equation_image:
#                     aws_path = f'https://{bucket_name}.s3.{ aws_region}.amazonaws.com/{folder_name}{book}/OEBPS/'
#                     img_url = aws_path + equation_image['src']
#                     print("This is equation image")
#                     img_key = img_url.replace(s3_base_url + "/", "")
#                     equation_image_path = download_aws_image(img_key)
#                     img = Image.open(equation_image_path)
#                     latex_text= latex_ocr(img)
#                     text_to_speech=latext_to_text_to_speech(latex_text)
#                     eqaution_data={'id': equation_Id, 'text':latex_text, 'text_to_speech':text_to_speech}
#                     print("this is equation image")
#                 else:
#                     equation_text = child.get_text(strip=True)
#                     eqaution_data = {'id': equation_Id,
#                                      'text': equation_text}
#                 if section_data:
#                     section_data[-1]['content'] += '{{equation:' + \
#                         equation_Id + '}} '
#                     if 'equations' in section_data[-1]:
#                         section_data[-1]['equations'].append(eqaution_data)
#                     else:
#                         section_data[-1]['equations'] = [eqaution_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{equation:' + equation_Id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = [eqaution_data]

#             # 100% equation image
#             elif child.name == 'div' and 'imagewrap' in child.get('class', []) and not child.find('p'):
#                 equation_Id = uuid.uuid4().hex
#                 equation_image = child.find('img')
#                 if book == "Basic Electrical and Electronics Engineering (9789332579170)" and equation_image and equation_image.get('id', '').startswith('eq'):
#                     aws_path = f'https://{bucket_name}.s3.{aws_region}.amazonaws.com/{folder_name}{book}/OEBPS/'
#                     img_url = aws_path + equation_image['src']
#                     print("This is equation image")
#                     img_key = img_url.replace(s3_base_url + "/", "")
#                     print(img_key)
#                     equation_image_path = download_aws_image(img_key)
#                     img = Image.open(equation_image_path)
#                     latex_text= latex_ocr(img)
#                     text_to_speech=latext_to_text_to_speech(latex_text)
#                     eqaution_data={'id': equation_Id, 'text':latex_text, 'text_to_speech':text_to_speech}
#                 else:
#                     aws_path = f'https://{bucket_name}.s3.{aws_region}.amazonaws.com/{folder_name}{book}/OEBPS/'
#                     img_url = aws_path+equation_image['src']
#                     print("this is equation image")
#                     img_key = img_url.replace(s3_base_url + "/", "")
#                     print(img_key)
#                     equation_image_path = download_aws_image(img_key)
#                     img = Image.open(equation_image_path)
#                     latex_text= latex_ocr(img)
#                     text_to_speech=latext_to_text_to_speech(latex_text)
#                     eqaution_data={'id': equation_Id, 'text':latex_text, 'text_to_speech':text_to_speech}
#                 if section_data:
#                     section_data[-1]['content'] += '{{equation:' + \
#                         equation_Id + '}} '
#                     if 'equations' in section_data[-1]:
#                         section_data[-1]['equations'].append(eqaution_data)
#                     else:
#                         section_data[-1]['equations'] = [eqaution_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{equation:' + equation_Id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = [eqaution_data]

#             # handling equaiton as text
#             elif child.name == 'math':
#                 print("equation here")
#                 equation_Id = uuid.uuid4().hex
#                 equation_text = child.get_text(strip=True)
#                 eqaution_data = {'id': equation_Id,
#                                  'text': equation_text}
#                 if section_data:
#                     section_data[-1]['content'] += '{{equation:' + \
#                         equation_Id + '}} '
#                     if 'equations' in section_data[-1]:
#                         section_data[-1]['equations'].append(eqaution_data)
#                     else:
#                         section_data[-1]['equations'] = [eqaution_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{equation:' + equation_Id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = []
#                     temp['equations'] = [eqaution_data]

#             elif child.name == 'pre':
#                 code_tags = child.find_all('code')
#                 code = ''
#                 if code_tags:
#                     code = ' '.join(code_tag.get_text(strip=True)
#                                     for code_tag in code_tags)
#                 else:
#                     code = child.get_text(strip=True)
#                 code_id = uuid.uuid4().hex
#                 code_data = {'id': code_id, 'code_snippet': code}
#                 if section_data:
#                     section_data[-1]['content'] += '{{code_snippet:' + code_id + '}} '
#                     if 'code_snippet' in section_data[-1]:
#                         section_data[-1]['code_snippet'].append(code_data)

#                     else:
#                         section_data[-1]['code_snippet'] = [code_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{code_snippet:' + code_id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = [code_data]
#                     temp['equations'] = []
#             # elif child.name == 'p' and 'center1' in child.get('class', []):

#             elif child.name == 'p' and 'programlisting' in child.get('class', []):
#                 # Handle p tags with class 'programlisting'
#                 print("code here")
#                 code = child.get_text(strip=True)
#                 code_id = uuid.uuid4().hex
#                 code_data = {'id': code_id, 'code_snippet': code}
#                 if section_data:
#                     section_data[-1]['content'] += '{{code_snippet:' + code_id + '}} '
#                     if 'code_snippet' in section_data[-1]:
#                         section_data[-1]['code_snippet'].append(code_data)
#                     else:
#                         section_data[-1]['code_snippet'] = [code_data]
#                 else:
#                     temp['title'] = ''
#                     temp['content'] = '{{code_snippet:' + code_id + '}} '
#                     temp['tables'] = []
#                     temp['figures'] = []
#                     temp['code_snippet'] = [code_data]
#                     temp['equations'] = []

#             elif elem.name == 'div' and elem.find('div', class_='mediaobject'):
#                 code_block = elem.find('div', class_='LineGroup')
#                 if code_block:
#                     fixed_lines = code_block.find_all(
#                         'div', class_='FixedLine')
#                     code = ' '.join(fixed_line.get_text()
#                                     for fixed_line in fixed_lines)
#                     code_id = uuid.uuid4().hex
#                     code_data = {'id': code_id, 'code_snippet': code}
#                     if section_data:
#                         section_data[-1]['content'] += '{{code_snippet:' + code_id + '}} '
#                         if 'code_snippet' in section_data[-1]:
#                             section_data[-1]['code_snippet'].append(code_data)
#                         else:
#                             section_data[-1]['code_snippet'] = [code_data]
#                     else:
#                         temp = {}
#                         temp['title'] = ''
#                         temp['content'] = '{{code_snippet:' + code_id + '}} '
#                         temp['tables'] = []
#                         temp['figures'] = []
#                         temp['code_snippet'] = [code_data]
#                         temp['equations'] = []

#             elif child.contents:
#                 section_data = extract_data(
#                     child, book, filename, db, section_data=section_data)
#         if temp:
#             section_data.append(temp)
#     return section_data


# @timeit
# def get_book_data(book):
#     print("Book Name >>> ", book)
#     db = mongo_init(mongo_connection_string)
#     toc = []
#     # check if book exists in db toc collection
#     db_toc = db['oct_toc'].find_one({'book': book})
#     if db_toc:
#         toc = db_toc['toc']
#     if not toc:
#         error = ''
#         try:
#             # get table of content
#             toc_content = get_file_object_aws(book, 'toc.ncx')
#             if toc_content:
#                 toc = get_toc_from_ncx(toc_content)
#             else:
#                 toc_content = get_file_object_aws(book, 'toc.xhtml')
#                 if toc_content:
#                     toc = get_toc_from_xhtml(toc_content)
#         except Exception as e:
#             error = str(e)
#             print(f'Error while parsing {book} toc >> {e}')
#         if not toc:
#             db['oct_no_toc'].insert_one({'book': book, 'error': error})
#         else:
#             db['oct_toc'].insert_one({'book': book, 'toc': toc})

#     files = []

#     for label, content in toc:
#         content_split = content.split('#')
#         if len(content_split) > 0:
#             filename = content_split[0]
#             if filename not in files:
#                 file_in_error = db['files_with_error'].find_one(
#                     {'book': book, 'filename': filename})
#                 if file_in_error:
#                     db['files_with_error'].delete_one(
#                         {'book': book, 'filename': filename})
#                 chapter_in_db = db['oct_chapters'].find_one(
#                     {'book': book, 'filename': filename})
#                 if chapter_in_db:
#                     if chapter_in_db['sections']:
#                         continue
#                     elif not chapter_in_db['sections']:
#                         db['oct_chapters'].delete_one(
#                             {'book': book, 'filename': filename})
#                 # print(label, content)
#                 html_content = get_file_object_aws(book, filename)

#                 if html_content:
#                     try:

#                         json_data = parse_html_to_json(
#                             html_content, book, filename, db)
#                         db['oct_chapters'].insert_one(
#                             {'book': book, 'filename': filename, 'sections': json_data})
#                     except Exception as e:
#                         print(f'Error while parsing {filename} html >> {e}')
#                         db['files_with_error'].insert_one(
#                             {'book': book, 'filename': filename, 'error': e})
#                         # clear mongo
#                         db['oct_chapters'].delete_many({'book': book})
#                 else:
#                     print('no html content found : ', filename)
#                     db['files_with_error'].insert_one(
#                         {'book': book, 'filename': filename, 'error': 'no html content found'})
#                 files.append(filename)


# def check_if_opf_exists(bucket_name, folder_name, book):
#     '''
#       Check if opf file exists in book folder

#       Args:
#           book (str): The name of the book.

#       Returns:
#           files: list of files with .opf extension

#       Raises:
#           botocore.exceptions.NoCredentialsError: If AWS credentials are not found or are invalid.
#           botocore.exceptions.ParamValidationError: If the provided bucket or folder name is invalid.
#           botocore.exceptions.EndpointConnectionError: If a connection to the AWS S3 service cannot be established.
#     '''
#     responses = []
#     continuation_token = None
#     while True:
#         if continuation_token:
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}{book}/OEBPS/', ContinuationToken=continuation_token)
#         else:
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}{book}/OEBPS/')
#         if 'Contents' in response:
#             if not responses:
#                 responses = response['Contents']
#             else:
#                 responses.extend(response['Contents'])
#         if 'IsTruncated' in response and response['IsTruncated']:
#             continuation_token = response['NextContinuationToken']
#         else:
#             break
#     files = []
#     if responses:
#         for obj in responses:
#             if len(obj['Key'].split('/')) == 4 and obj['Key'].endswith('.opf'):
#                 files.append(obj['Key'].split('/')[-1])
#     return files


# def get_heading_tags(elem):
#     h_tag = []
#     for child in elem.children:
#         if child.name:
#             if child.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
#                 h_tag.append(child.name)
#             elif child.contents:
#                 h_tag = get_heading_tags(child, h_tag)
#     return h_tag


# def clean_string(html_string):
#     # Unescape HTML entities
#     unescaped_string = html.unescape(html_string)
#     # Remove HTML tags
#     clean_text = re.sub('<[^<]+?>', '', unescaped_string)
#     # Strip leading/trailing whitespaces
#     clean_text = clean_text.strip()
#     return clean_text


# def get_all_books_info(bucket_name, folder_name):
#     book_info_saved = []
#     book_info_not_saved = []
#     books = get_all_books_names(bucket_name, folder_name)
#     print(len(books))
#     db = mongo_init(mongo_connection_string)
#     for book in books:
#         try:
#             opf_files = check_if_opf_exists(bucket_name, folder_name, book)
#             for opf_file in opf_files:
#                 opf_content = get_file_object_aws(book, opf_file)
#                 if opf_content:
#                     book_info = get_book_info(opf_content)
#                     book_info['book'] = book
#                     db['oct_basic_info'].insert_one(book_info)
#                     book_info_saved.append(book)
#                     break
#             if book not in book_info_saved:
#                 book_info_not_saved.append(book)
#         except Exception as e:
#             print(f'Error while parsing {book} >> {e}')
#             book_info_not_saved.append(book)
#     return book_info_saved, book_info_not_saved

# # if __name__ == '__main__':
# #     # get_book_data('A Field Guide to Digital Transformation (9780137571871)')
# #     # get_book_data('Accounting For Dummies 7th Edition (9781119837527)')
# #     # get_book_data('This is Learning Experience Design (9780138206307)')
# #     # get_book_data('Mastering API Architecture (9781492090625)')
# #     # get_book_data('How to Build Android Apps with Kotlin - Second Edition (9781837634934)')
# #     books = get_all_books_names('bud-datalake', 'Books/Oct29-1/')
# #     print(len(books))
# #     for book_number, book in enumerate(books, start=1):
# #         print(f"Processing book {book_number}")
# #         get_book_data(book)

# #     saved, not_saved = get_all_books_info('bud-datalake', 'Books/Oct29-1/')
# #     print(len(saved))
# #     print(len(not_saved))
# #     print(not_saved)
# #     # get_book_data('Advanced Python 3 Programming Techniques (9780321637727)')
# #     # pass


# # @timeit
# # def process_all_books():
# #     books = get_all_books_names('bud-datalake', 'Books/Oct29-1/')
# #     print(len(books))
# #     for book_number, book in enumerate(books, start=1):
# #         print(f"Processing book {book_number} , {book}")
# #         get_book_data(book)

# #     saved, not_saved = get_all_books_info('bud-datalake', 'Books/Oct29-1/')
# #     print(len(saved))
# #     print(len(not_saved))
# #     print(not_saved)


# # process_all_books()


# # get_book_data('AC Circuits and Power Systems in Practice (9781118924594)')
