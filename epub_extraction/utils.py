import os
import re
from dotenv import load_dotenv
import boto3
from pymongo import MongoClient
import boto3
from latext import latex_to_text
from uuid import uuid4
import html
import time
from functools import wraps
from dotenv import load_dotenv
from pymongo import MongoClient
import xml.etree.ElementTree as ET
from lxml import etree
from bs4 import BeautifulSoup, NavigableString
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



def timeit(func):
    """
    Keeps track of the time taken by a function to execute.
    """
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

def mongo_init(databas_name):
    client = MongoClient(mongo_connection_string)
    db=client[databas_name]
    return db


def generate_unique_id():
    """ generate unique id """
    return uuid4().hex


def get_s3():
    return s3

def get_all_books_names(bucket_name, folder_name):
    all_books = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder_name,
                Delimiter='/',
                ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder_name,
                Delimiter='/'
            )

        # Process the current page of objects
        books_on_page = [each['Prefix'].split('/')[-2] for each in response['CommonPrefixes']]
        all_books.extend(books_on_page)

        # Check if there are more pages to retrieve
        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break

    return all_books

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

def get_file_object_aws(book, filename, folder_name, bucket_name):
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

def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text)
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech