
import os
import json
import requests
import math
import uuid
from bs4 import BeautifulSoup

def parse_html_table(html):
    data = []
    
    # Create a BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(html, 'html.parser')
    
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

def find_distance(point1, point2):
    return math.sqrt((point1[0] - point2[0])**2 + (point1[1] - point2[1])**2)

def find_closest_bbox(layout_bbox, result_array):
    min_distance = float('inf')
    closest_result = None
    
    for result_item in result_array:
        for bbox_and_value in result_item:
            result_bbox = bbox_and_value[0][0]
            distance = find_distance(layout_bbox, result_bbox)
            if distance < min_distance:
                min_distance = distance
                closest_result = bbox_and_value
                
    return closest_result

def find_closest_results_for_table_caption(data):
    layout_objects = data["layout"]
    result_array = data["result"]
    closest_values = []
    
    for layout_obj in layout_objects:
        if layout_obj["type"] == "table_caption" and "bbox" in layout_obj:
            layout_bbox = layout_obj["bbox"]
            closest_result = find_closest_bbox(layout_bbox, result_array)
            if closest_result:
                closest_value = closest_result[1][0]
                closest_values.append(closest_value)
    
    return closest_values
# Example data

def process_book_page(image_path):

    files = {
        'file': (image_path, open(image_path, 'rb'))
    }
    response = requests.post('http://91.203.132.119:8000/ocr', files=files)

    if response.status_code == 200:
        data = response.json()
        if data:
            print("process fisrt page image")
        # Now you can use the image_data as needed
    else:
        print('API request failed with status code:', response.status_code)
    tables = extract_table_results(data)
    caption = find_closest_results_for_table_caption(data)
    pageTable=[]
    for idx, table_data in enumerate(tables):
        caption = caption[idx] if idx < len(caption) else ""
        # if should_skip_table(table_data):
        #     continue
        table_id = uuid.uuid4().hex
        rows = [row for row in table_data]
        pageTable.append({
            "id": table_id,
            "caption": caption,
            "data": {
                "rows": rows
                }
            })

    return pageTable
