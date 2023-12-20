
import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
load_dotenv()

def parse_html_table(html):
    data = []
    
    # Create a BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(html, 'html.parser')
    print(html)
    
    # Find the table within the HTML
    table = soup.find('table')
    
    # Find all rows within the table
    rows = table.find_all('tr')
    
    # Loop through rows and extract cell data
    for row in rows:
        cells = row.find_all(['td', 'th'])
        row_data = [cell.get_text(strip=True) for cell in cells]
        data.append(row_data)
    
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
    print(files)
    response = requests.post(os.environ['BUD_OCR'], files=files)
    print(response)
    if response.status_code == 200:
        data = response.json()
        tables = extract_table_results(data)
        for idx, table_data in enumerate(tables):
            rows = [row for row in table_data]
            data= {
                "rows": rows
            }
            return data
    else:
        print('API request failed with status code:', response.status_code)
        return None
