import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from utils import parse_table

load_dotenv()

def parse_html_table(html):
    data = []
    # Create a BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(html, 'html.parser')    
    # Find the table within the HTML
    table = soup.find('table')
    
    # Extract header
    header = [th.text.strip() for th in table.select('thead tr td')]

    # Extract rows
    rows = [[td.text.strip() for td in row.select('td')] for row in table.select('tbody tr')]

    header_arrays = [[item] for item in header]

    # Create a dictionary
    data = {'header': header_arrays, 'rows': rows}

    return data
  
def extract_table_results(data):
    layout_objects = data["layout"]
    table_results = []

    for layout_obj in layout_objects:
        if layout_obj["type"] == "table":
            table_html = layout_obj["res"]["html"]  # Access the HTML content of the table
            parsed_table = parse_html_table(table_html)
            table_results.append(parsed_table)
    
    return table_results

def process_book_page(image_path):
    files = {
        'file': (image_path, open(image_path, 'rb'))
    }
    response = requests.post(os.environ['BUD_OCR'], files=files)
    print(response)
    if response.status_code == 200:
        data = response.json()
        tables = extract_table_results(data)
        if tables:
            return tables[0]
        else:
            return {}
    else:
        print('API request failed with status code:', response.status_code)

# process_book_page('/home/bud-data-extraction/datapipeline/pdf_extraction_pipeline/epub_extraction/page_41.jpg')