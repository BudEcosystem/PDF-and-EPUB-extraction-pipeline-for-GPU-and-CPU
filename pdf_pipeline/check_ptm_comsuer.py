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
from pdf_producer import page_extraction_queue, book_completion_queue
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

def check_ptm_status(ch, method, properties, body):
    try:
        message = json.loads(body)
        job = message['job']
        bookname = message["bookname"]
        bookId = message["bookId"]

        publeynet_done_document = publaynet_done.find_one({"bookId": bookId})
        table_done_document = table_bank_done.find_one({"bookId": bookId})
        mfd_done_document = mfd_done.find_one({"bookId": bookId})
        pdFigCap = figure_caption.find_one({"bookId": bookId})
        if publeynet_done_document and table_done_document and mfd_done_document and pdFigCap:
            collections = [publaynet_book_job_details, table_bank_book_job_details, mfd_book_job_details]
            page_results = {}
            for collection in collections:
                document = collection.find_one({"bookId": bookId})
                if document:
                    for page in document.get("pages", []):
                        page_num = page["page_num"]
                        result_array = page.get("result", [])
                        image_path = page.get("image_path", "")
                        for result in result_array:
                            result["image_path"] = image_path

                        # Initialize an empty list for the page if it doesn't exist in the dictionary
                        if page_num not in page_results:
                            page_results[page_num] = []

                        # Append the results to the page-specific list
                        page_results[page_num].extend(result_array)
            page_extraction_queue('page_extraction_queue', page_results, bookname, bookId)          
        else:
            print("not yet completed")
    except Exception as e:
        print(e)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_ptm_completion_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='check_ptm_completion_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='check_ptm_completion_queue', on_message_callback=check_ptm_status)

        print(' [*] Waiting for messages on check_ptm_completion_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
   


if __name__ == "__main__":
    try:
        consume_ptm_completion_queue()
        
    except KeyboardInterrupt:
        pass