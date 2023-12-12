# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import pytesseract
import traceback
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
from PIL import Image
import os
import boto3
import re
import pymongo
import cv2
import uuid
from tablecaption import process_book_page
from utils import timeit, crop_image
import json
from pdf_producer import book_completion_queue, error_queue, table_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

# Configure AWS credentials
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']

# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']


client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
book_other_pages=db.book_other_pages
book_other_pages_done=db.book_other_pages_done

def extract_other_pages(ch, method, properties, body):
    try:
        message = json.loads(body)
        total_other_pages=message['total_other_pages']
        pages_result=message['page_result']
        bookname = message["bookname"]
        bookId = message["bookId"]
        page_num=message['page_num']
        page_obj= process_pages(pages_result, bookname, bookId)
        document=book_other_pages.find_one({'bookId':bookId})
        if document:
            book_other_pages.update_one({"_id":document["_id"]}, {"$push": {"pages": page_obj}})
        else:
            new_book_document = {
                "bookId": bookId,
                "book": bookname,  
                "pages": [page_obj]
            }
            book_other_pages.insert_one(new_book_document)
        if total_other_pages==page_num+1:
            book_other_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"other pages Done"})
            book_completion_queue("book_completion_queue",bookname, bookId)
    except Exception as e:
        error = {"consumer":"other_pages","page_num":page_num, "error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue',bookname, bookId,error)    
    finally:
        print("ack received")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def process_pages(page, bookname, bookId):
    page_tables=[]
    page_figures=[]
    page_equations=[]
    results = page.get("results", [])
    image_path = page.get("image_path", "")
    pdFigCap = page.get("pdFigCap", False)
    page_num = page.get("page_num", "")
    page_content= sort_text_blocks_and_extract_data(results, image_path,page_figures,page_equations, pdFigCap, bookname, bookId, page_num)
    page_obj={
        "page_num":page_num,
        "content":page_content,
        "tables":page_tables,
        "figures":page_figures,
        "equations":page_equations
        }
    return page_obj

def sort_text_blocks_and_extract_data(blocks, imagepath, page_figures, page_equations, pdFigCap, bookname, bookId, page_num):
    sorted_blocks = sorted(blocks, key=lambda block: (block['y_1'] + block['y_2']) / 2)
    output = ""
    prev_block = None
    next_block = None
    for i, block in enumerate(sorted_blocks): 
        if i > 0:
            prev_block = sorted_blocks[i - 1]
        if i < len(sorted_blocks) - 1:
            next_block = sorted_blocks[i + 1]  
        if block['type'] == "Table":
            output = process_table(block,imagepath, output, bookname, bookId, page_num)
        elif block['type'] == "Figure":
            if pdFigCap:
                output = process_figure(block, imagepath, output, page_figures)
            else:
                output=process_publeynet_figure(block, imagepath, prev_block, next_block, output, page_figures)  
        elif block['type'] == "Text":
            output = process_text(block, imagepath, output)
        elif block['type'] == "Title":
            output = process_title(block, imagepath, output)
        elif block['type'] == "List":
            output = process_list(block, imagepath, output)

    page_content = re.sub(r'\s+', ' ', output).strip()
    return page_content

@timeit
def process_table(table_block,imagepath, output,bookname, bookId, page_num):
    x1, y1, x2, y2 = table_block['x_1'], table_block['y_1'], table_block['x_2'], table_block['y_2']
    img = cv2.imread(imagepath)
    y1 -= 70
    if y1 < 0:
        y1 = 0
    x1 = 0
    x2 += 20
    if x2 > img.shape[1]:
        x2 = img.shape[1]
    y2 += 20
    if y2 > img.shape[0]:
        y2 = img.shape[0]
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    tableId = uuid.uuid4().hex
    table_image_path =os.path.abspath(f"cropped_table{tableId}.png")
    cv2.imwrite(table_image_path, cropped_image)
    with open(table_image_path, 'rb') as img:
        img_data = img.read()
    image_data_base64 = base64.b64encode(img_data).decode('utf-8')
    data = {'img': image_data_base64}
    output += f"{{{{table:{tableId}}}}}"
    table_queue('table_queue',tableId,data,page_num,bookname,bookId)
    return output

@timeit
def process_figure(figure_block, imagepath, output, page_figures):
    figureId = uuid.uuid4().hex
    figure_image_path = crop_image(figure_block,imagepath, figureId)
    output += f"{{{{figure:{figureId}}}}}"

    figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":figure_url,
        "caption": figure_block['caption']
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

@timeit
def process_publeynet_figure(figure_block, imagepath, prev_block, next_block, output, page_figures):
    caption=""
    figureId = uuid.uuid4().hex
    figure_image_path =crop_image(figure_block,imagepath, figureId)
    print(figure_image_path)
    output += f"{{{{figure:{figureId}}}}}"

    if prev_block:
        prevId=uuid.uuid4().hex
        prev_image_path = crop_image(prev_block,imagepath, prevId)
        #extraction of text from cropped image using pytesseract
        image =Image.open(prev_image_path)
        text = pytesseract.image_to_string(image)
        text = re.sub(r'\s+', ' ', text).strip()
        pattern = r"(Fig\.|Figure)\s+\d+"
        match = re.search(pattern, text)
        if match:
            caption = text
        if os.path.exists(prev_image_path):
            os.remove(prev_image_path)

    if next_block:
        nextId=uuid.uuid4().hex
        next_image_path = crop_image(next_block,imagepath, nextId) 
        #extraction of text from cropped image using pytesseract
        image =Image.open(next_image_path)
        text = pytesseract.image_to_string(image)
        text = re.sub(r'\s+', ' ',text).strip()
        pattern = r"(Fig\.|Figure)\s+\d+"
        match = re.search(pattern, text)
        if match:
            caption = text
        if os.path.exists(next_image_path):
            os.remove(next_image_path)

    figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":figure_url,
        "caption":caption
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

@timeit
def process_text(text_block,imagepath, output):
    textId=uuid.uuid4().hex
    cropped_image_path = crop_image(text_block,imagepath, textId)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_title(title_block,imagepath, output):
   
    titleId=uuid.uuid4().hex
    cropped_image_path = crop_image(title_block,imagepath, titleId)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_list(list_block,imagepath, output):
    listId=uuid.uuid4().hex
    cropped_image_path = crop_image(list_block,imagepath, listId)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

 
@timeit
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text.lstrip('\\'))
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech

@timeit
def upload_to_aws_s3(figure_image_path, figureId): 
    folderName=os.environ['AWS_PDF_IMAGE_UPLOAD_FOLDER']
    s3_key = f"{folderName}/{figureId}.png"
    # Upload the image to the specified S3 bucket
    s3.upload_file(figure_image_path, bucket_name, s3_key)
    # Get the URL of the uploaded image
    figure_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"

    return figure_url 

def consume_other_pages_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='other_pages_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='other_pages_queue', on_message_callback=extract_other_pages)

        print(' [*] Waiting for messages on other_pages_queue To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()

   


if __name__ == "__main__":
    try:
        consume_other_pages_queue()      
    except KeyboardInterrupt:
        pass