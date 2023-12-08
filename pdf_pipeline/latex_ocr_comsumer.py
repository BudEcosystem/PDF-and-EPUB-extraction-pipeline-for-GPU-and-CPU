# pylint: disable=all
# type: ignore
import numpy as np
from dotenv import load_dotenv
import subprocess
import pytesseract
import traceback
import pytz
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
from PIL import Image
import os
import PyPDF2
import img2pdf
import fitz
import shutil
import boto3
import re
import cv2
import pymongo
from urllib.parse import urlparse
import urllib
import uuid
from latext import latex_to_text
from pix2tex.cli import LatexOCR
from PyPDF2 import PdfReader
from tablecaption import process_book_page
from utils import timeit, crop_image
import pika
import json
from pdf_producer import book_completion_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

# latex_ocr_model = LatexOCR()
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

model = LatexOCR()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
bookdata = db.book_set_2_new
error_collection = db.error_collection
figure_caption = db.figure_caption
table_bank_done=db.table_bank_done
publaynet_done=db.publaynet_done
mfd_done=db.mfd_done
publaynet_book_job_details=db.publaynet_book_job_details
table_bank_book_job_details=db.table_bank_book_job_details
mfd_book_job_details=db.mfd_book_job_details
latex_pages=db.latex_pages
latex_pages_done=db.latex_pages_done

def extract_latex_pages(ch, method, properties, body):
    try:
        message = json.loads(body)
        total_latex_pages=message['total_latex_pages']
        pages_result=message['page_result']
        bookname = message["bookname"]
        bookId = message["bookId"]
        page_num=message['page_num']
        page_obj= process_pages(pages_result)
        document=latex_pages.find_one({'bookId':bookId})
        if document:
            latex_pages.update_one({"_id":document["_id"]}, {"$push": {"pages": page_obj}})
        else:
            new_book_document = {
                "bookId": bookId,
                "book": bookname,  
                "pages": [page_obj]
            }
            latex_pages.insert_one(new_book_document)

        if total_latex_pages==page_num+1:
            latex_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"latex pages Done"})
            book_completion_queue("book_completion_queue",bookname, bookId)
    except Exception as e:
        print(e)
    finally:
        print("ack received")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    


def process_pages(page):
    try:
        page_tables=[]
        page_figures=[]
        page_equations=[]
        results = page.get("results", [])
        image_path = page.get("image_path", "")
        pdFigCap = page.get("pdFigCap", False)
        page_num = page.get("page_num", "")
        page_content= sort_text_blocks_and_extract_data(results, image_path,page_tables,page_figures,page_equations, pdFigCap)
        page_obj={
            "page_num":page_num,
            "content":page_content,
            "tables":page_tables,
            "figures":page_figures,
            "equations":page_equations
        }
        return page_obj
    except Exception as e:
        print("error while page",e)

def sort_text_blocks_and_extract_data(blocks, imagepath, page_tables, page_figures, page_equations, pdFigCap):
    try:
        print("hello")
        sorted_blocks = sorted(blocks, key=lambda block: (block['y_1'] + block['y_2']) / 2)
        print(sorted_blocks)
        output = ""
        prev_block = None
        next_block = None
        for i, block in enumerate(sorted_blocks): 
            if i > 0:
                prev_block = sorted_blocks[i - 1]
            if i < len(sorted_blocks) - 1:
                next_block = sorted_blocks[i + 1]  
            if block['type'] == "Table":
                output = process_table(imagepath, output, page_tables)
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
            elif block['type']=='Equation':
                output=process_equation(block, imagepath, output, page_equations)

        page_content = re.sub(r'\s+', ' ', output).strip()
        return page_content
    except Exception as e:
        print('error while sorting,',e)


@timeit
def process_table(imagepath, output, page_tables):
    try:
        output=process_book_page(imagepath,page_tables, output)
        return output
    except Exception as e:
        print("error procwss",e)

@timeit
def process_figure(figure_block, imagepath, output, page_figures):
    try:
        figureId = uuid.uuid4().hex
        figure_image_path = crop_image(figure_block,imagepath, figureId)
        output += f"{{{{figure:{figureId}}}}}"

    # figure_url=upload_to_aws_s3(figure_image_path, figureId)
        page_figures.append({
            "id":figureId,
            "url":"figure_url",
            "caption": figure_block['caption']
        })
        if os.path.exists(figure_image_path):
            os.remove(figure_image_path)
        return output  
    except Exception as e:
        print("error while figure",e)  

@timeit
def process_publeynet_figure(figure_block, imagepath, prev_block, next_block, output, page_figures):
    print("publeynbeje")
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

    # figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":'figure_url',
        "caption":caption
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

@timeit
def process_text(text_block,imagepath, output):
    try:
        textId=uuid.uuid4().hex
        cropped_image_path = crop_image(text_block,imagepath, textId)
        image =Image.open(cropped_image_path)
        text = pytesseract.image_to_string(image)
        output+=text
        if os.path.exists(cropped_image_path):
            os.remove(cropped_image_path)
        return output
    except Exception as e:
        print("error while process text",e)  
    
    
    

@timeit
def process_title(title_block,imagepath, output):
    try:
        titleId=uuid.uuid4().hex
        cropped_image_path = crop_image(title_block,imagepath, titleId)
        #extraction of text from cropped image using pytesseract
        image =Image.open(cropped_image_path)
        text = pytesseract.image_to_string(image)
        output+=text
        if os.path.exists(cropped_image_path):
            os.remove(cropped_image_path)
        return output
    except Exception as e:
        print("error while process title",e)  
    

@timeit
def process_list(list_block,imagepath, output):
    try:
        listId=uuid.uuid4().hex
        cropped_image_path = crop_image(list_block,imagepath, listId)
        image =Image.open(cropped_image_path)
        text = pytesseract.image_to_string(image)
        output+=text
        if os.path.exists(cropped_image_path):
            os.remove(cropped_image_path)
        return output
    except Exception as e:
        print("error while process list",e)  
    
@timeit
def process_equation(equation_block, imagepath, output, page_equations):
    try:
        print("hello")
        equationId=uuid.uuid4().hex
        equation_image_path = crop_image(equation_block,imagepath, equationId)
        print(equation_image_path)
        output += f"{{{{equation:{equationId}}}}}"
        img = Image.open(equation_image_path)
        print(img)
        latex_text= model(img)
        text_to_speech=latext_to_text_to_speech(latex_text)
        page_equations.append(
            {'id': equationId, 'text':latex_text, 'text_to_speech':text_to_speech}
            )
        if os.path.exists(equation_image_path):
            os.remove(equation_image_path)
        return output
    except Exception as e:
        print("error while equation",e)  
 

@timeit
def latext_to_text_to_speech(text):
    try:
        text = "${}$".format(text.lstrip('\\'))
        text_to_speech = latex_to_text(text)
        return text_to_speech
    except Exception as e:
        print('error while text to speech',e)


def consume_latex_ocr_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='latex_ocr_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='latex_ocr_queue', on_message_callback=extract_latex_pages)

        print(' [*] Waiting for messages on latec_ocr_queue To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
   


if __name__ == "__main__":
    try:
        consume_latex_ocr_queue()      
        # extract_latex_pages()
    except KeyboardInterrupt:
        pass