# pylint: disable=all
# type: ignore
import numpy as np
from dotenv import load_dotenv
import subprocess
import pytesseract
import traceback
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
from pdf_producer import nougat_queue, book_completion_queue, other_pages_queue, latex_ocr_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()



latex_ocr_model = LatexOCR()
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
bookdata = db.book_set_2_new
error_collection = db.error_collection
figure_caption = db.figure_caption
table_bank_done=db.table_bank_done
publaynet_done=db.publaynet_done
mfd_done=db.mfd_done
publaynet_book_job_details=db.publaynet_book_job_details
table_bank_book_job_details=db.table_bank_book_job_details
mfd_book_job_details=db.mfd_book_job_details


def extract_pages(ch, method, properties, body):
    try:
        message = json.loads(body)
        page_results=message['book_pages']
        bookname = message["bookname"]
        bookId = message["bookId"]
        nougat_pages = []
        other_pages=[]
        latex_ocr_pages=[]

        for page_num_str, results in page_results.items():
            page_num=int(page_num_str)
            process_page_result(results, page_num, bookId, bookname, nougat_pages, other_pages,latex_ocr_pages) 
        
        # //send other page to other_pages_queue
        other_pages_queue('other_pages_queue', other_pages, bookname, bookId)   

        # send latex_ocr_pages to latex_ocr_queue
        # latex_ocr_queue('latex_ocr_queue',latex_ocr_pages,bookname,bookId)

        # send nougat_pages to nougat_queue   
        # total_nougat_pages = len(nougat_pages)
        # for page_num, page in enumerate(nougat_pages):
            # nougat_queue('nougat_queue', page['image_path'], total_nougat_pages, page['page_num'], page_num,page['bookname'], page['bookId'])

    except Exception as e:
        error={"bookId":{bookId},"book":{bookname},"error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
        print(error)    
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

def process_page_result(results, page_num, bookId,bookname,nougat_pages,other_pages,latex_ocr_pages):
    image_path=results[0]['image_path']
    process_page(results,image_path, page_num, bookId,bookname, nougat_pages, other_pages,latex_ocr_pages)

def process_page(results, image_path, page_num, bookId,bookname,nougat_pages,other_pages,latex_ocr_pages):
    try:
        print(results)
        pdFigCap = False
       # Check the status in the figure_caption collection
        document_status = figure_caption.find_one({"bookId": bookId, "status": "success"})
        if document_status:
            pdFigCap = True
            results = [block for block in results if block['type'] != 'Figure']
            figures_block = []
            for page in document_status.get("pages", []):
                if page.get("page_num") == page_num + 1:
                    figure_bbox_values = page.get("figure_bbox")
                    caption_text = page.get('caption_text')
                    caption = ''.join(caption_text)

                    old_page_width = 439
                    old_page_height = 666
                    new_page_width = 1831
                    new_page_height = 2776

                    width_scale = new_page_width / old_page_width
                    height_scale = new_page_height / old_page_height

                    x1, y1, x2, y2 = figure_bbox_values

                    x1 = x1 * width_scale
                    y1 = y1 * height_scale
                    x2 = x2 * width_scale
                    y2 = y2 * height_scale

                    x2 = x1 + x2
                    y2 = y1 + y2

                    figure_block = {
                        "x_1": x1,
                        "y_1": y1,
                        "x_2": x2,
                        "y_2": y2,
                        "type": "Figure",
                        "caption": caption
                    }
                    figures_block.append(figure_block)

            if figures_block:
                results.extend(figures_block)

        if not results or not any(block['type'] in ["Table", "Figure"] for block in results):
            nougat_pages.append({
                "image_path":image_path,
                "page_num": page_num,
                "bookname": bookname, 
                "bookId": bookId
            })
        elif any(block['type'] == "Equation" for block in results):
            latex_ocr_pages.append({
                "results": results,
                "image_path": image_path,
                "bookname": bookname,
                "bookId": bookId,
                "page_num": page_num,
                "pdFigCap": pdFigCap
            })
        else:
            other_pages.append({
                "results":results,
                "image_path":image_path,
                "bookname":bookname,
                "bookId":bookId,
                "page_num":page_num,
                "pdFigCap":pdFigCap
            })     
    except Exception as e:
        print(f"An error occurred while processing {bookname}, page {page_num}: {str(e)}, line_numbe {traceback.extract_tb(e.__traceback__)[-1].lineno}")


def consume_page_extraction_queue():
    try:    
        # Declare the queue
        channel.queue_declare(queue='page_extraction_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='page_extraction_queue', on_message_callback=extract_pages)

        print(' [*] Waiting for messages on page_extraction_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
   


if __name__ == "__main__":
    try:
        consume_page_extraction_queue()
    except KeyboardInterrupt:
        pass