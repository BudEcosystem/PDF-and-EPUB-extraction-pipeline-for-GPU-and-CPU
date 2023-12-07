# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import subprocess
import sys
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
from PIL import Image
import os
import PyPDF2
from PyPDF2 import PdfFileWriter, PdfFileReader

import requests
import img2pdf
import fitz
import traceback
import re
import pymongo
import uuid
from PyPDF2 import PdfReader
from latext import latex_to_text
import pika
import json

from pdf_producer import book_completion_queue, nougat_pdf_queue
from nougat.utils.checkpoint import get_checkpoint

from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

# CHECKPOINT = get_checkpoint('nougat')

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
error_collection = db.error_collection
nougat_pagess=db.nougat_pages
nougat_done=db.nougat_done

# @timeit
# def extract_text_equation_with_nougat():
#     try:
#         nougat_pages= [{'image_path': '/home/azureuser/prakash/output_1/page_1.jpg', 'page_num': 0, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_2.jpg', 'page_num': 1, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_3.jpg', 'page_num': 2, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_4.jpg', 'page_num': 3, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_5.jpg', 'page_num': 4, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_6.jpg', 'page_num': 5, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_7.jpg', 'page_num': 6, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_8.jpg', 'page_num': 7, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_9.jpg', 'page_num': 8, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_10.jpg', 'page_num': 9, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_11.jpg', 'page_num': 10, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_12.jpg', 'page_num': 11, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_13.jpg', 'page_num': 12, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_14.jpg', 'page_num': 13, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_15.jpg', 'page_num': 14, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_20.jpg', 'page_num': 19, 'bookname': 'output_1.pdf', 'bookId': '87665865'}]
#         bookname= "hello"
#         bookId="23445"
#         pages_numbers=[]
#         pdf_name ="nougat.pdf"
#         page_nums=[page['page_num'] for page in nougat_pages]
#         print(page_nums)
#         page_equations=[]
#         image_paths = [page['image_path'] for page in nougat_pages]
#         with open(pdf_name, 'wb') as pdf_file:
#             pdf_file.write(img2pdf.convert(image_paths))
        
#         message = get_nougat_text(pdf_name)
#         if message=='success':
#             path='pdfs'+'/'+pdf_name
#             directory = os.path.abspath(path)+'/pages'
#             files = sorted(os.listdir(directory))
#             for file_name in files:
#                 file_path = os.path.join(directory, file_name)
#                 with open(file_path, 'r', encoding='utf-8') as file:
#                     file_content = file.read()
                    
      
#         pattern = r'(\\\(.*?\\\)|\\\[.*?\\\])'
#         def replace_with_uuid(match):
#             equationId = uuid.uuid4().hex
#             match_text = match.group()
#             text_to_speech=latext_to_text_to_speech(match_text)
#             page_equations.append({'id': equationId, 'text': match_text, 'text_to_speech':text_to_speech})
#             return f'{{{{equation:{equationId}}}}}'
    
#         page_content = re.sub(pattern, replace_with_uuid, latex_text)
#         page_content = re.sub(r'\s+', ' ', page_content).strip()

#         page_object={
#             "page_num":page_num,
#             "text":page_content,
#             "tables":[],
#             "figures":[],
#             "page_equations":page_equations
#         }
#         # book_document = nougat_pages.find_one({"bookId":  bookId})
#         # if book_document:
#         #     nougat_pages.update_one({"_id": book_document["_id"]}, {"$push": {"pages": page_object}})
#         # else:
#         #     new_book_document = {
#         #     "bookId": bookId,
#         #     "book": bookname,  
#         #     "pages": [page_object]
#         #     }
#         #     nougat_pages.insert_one(new_book_document)
#         # if os.path.exists(pdf_path):
#         #     os.remove(pdf_path)
#         # if total_nougat_pages==page_num+1:
#         #     nougat_done.insert_one({"bookId":bookId,"book":bookname,"status":"nougat pages Done"})
#         #     book_completion_queue("book_completion_queue",bookname, bookId)
#     except Exception as e:
#         print(f'error occured while processing of book  though nougat {str(e)}') 
#     finally:
#         print('hello')

import os
from PyPDF2 import PdfReader, PdfWriter

def split_pdf(pdf_path, max_pages=5):
    output_directory='nougat_pdfs'
    os.makedirs(output_directory, exist_ok=True)
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PdfReader(pdf_file)
        total_pages = len(pdf_reader.pages)

        if total_pages <= max_pages:
            # No need to split
            return [pdf_path]

        pdf_paths = []
        for start_page in range(0, total_pages, max_pages):
            end_page = min(start_page + max_pages, total_pages)
            pdf_writer = PdfWriter()

            for page_num in range(start_page, end_page):
                pdf_writer.add_page(pdf_reader.pages[page_num])

            split_pdf_name = os.path.basename(pdf_path).replace('.pdf', f'_part_{start_page}_{end_page}.pdf')
            split_pdf_path = os.path.join(output_directory, split_pdf_name)
            pdf_paths.append(split_pdf_path)

            with open(split_pdf_path, 'wb') as split_pdf_file:
                pdf_writer.write(split_pdf_file)

        return pdf_paths

@timeit
def extract_text_equation_with_nougat():
    try:
        get_nougat_text('output_1.pdf')
        # nougat_pages= [{'image_path': '/home/azureuser/prakash/output_1/page_1.jpg', 'page_num': 0, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_2.jpg', 'page_num': 1, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_3.jpg', 'page_num': 2, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_4.jpg', 'page_num': 3, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_5.jpg', 'page_num': 4, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_6.jpg', 'page_num': 5, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_7.jpg', 'page_num': 6, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_8.jpg', 'page_num': 7, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_9.jpg', 'page_num': 8, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_10.jpg', 'page_num': 9, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_11.jpg', 'page_num': 10, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_12.jpg', 'page_num': 11, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_13.jpg', 'page_num': 12, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_14.jpg', 'page_num': 13, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_15.jpg', 'page_num': 14, 'bookname': 'output_1.pdf', 'bookId': '87665865'}, {'image_path': '/home/azureuser/prakash/output_1/page_20.jpg', 'page_num': 19, 'bookname': 'output_1.pdf', 'bookId': '87665865'}]
        # bookname = "hello"
        # bookId = "23445"
        # # pages_numbers = []
        # pdf_name = "nougat.pdf"
        # # page_nums = [page['page_num'] for page in nougat_pages]
        # # print(page_nums)
       
        # image_paths = [page['image_path'] for page in nougat_pages]
        # with open(pdf_name, 'wb') as pdf_file:
        #     pdf_file.write(img2pdf.convert(image_paths))

        # pdf_paths = split_pdf('/home/azureuser/prakash/output_1.pdf')
        
        # pdf_paths=['nougat_part_0_5.pdf','nougat_part_5_10.pdf','nougat_part_10_15.pdf','nougat_part_15_16.pdf']
        # for pdf_path in pdf_paths:
        #     nougat_pdf_queue('nougat_pdf_queue',pdf_path,bookname,bookId)
        
            # if message=='success':
            #     path = 'pdfs' + '/' + pdf_path
            #     directory = os.path.abspath(path) + '/pages'
            #     files = sorted(os.listdir(directory))
            #     for file_name, page_num in zip(files, page_nums):
            #         page_equations = []
            #         file_path = os.path.join(directory, file_name)
            #         with open(file_path, 'r', encoding='utf-8') as file:
            #             file_content = file.read()

            #         pattern = r'(\\\(.*?\\\)|\\\[.*?\\\])'
            #         def replace_with_uuid(match):
            #             equationId = uuid.uuid4().hex
            #             match_text = match.group()
            #             text_to_speech = latext_to_text_to_speech(match_text)
            #             page_equations.append({'id': equationId, 'text': match_text, 'text_to_speech': text_to_speech})
            #             return f'{{{{equation:{equationId}}}}}'

            #         page_content = re.sub(pattern, replace_with_uuid, file_content)
            #         page_content = re.sub(r'\s+', ' ', page_content).strip()

            #         page_object = {
            #             "page_num": page_num,
            #             "text": page_content,
            #             "tables": [],
            #             "figures": [],
            #             "page_equations": page_equations
            #         }
            #         book_document = nougat_pagess.find_one({"bookId":  bookId})
            #         if book_document:
            #             nougat_pagess.update_one({"_id": book_document["_id"]}, {"$push": {"pages": page_object}})
            #         else:
            #             new_book_document = {
            #                 "bookId": bookId,
            #                 "book": bookname,  
            #                 "pages": [page_object]
            #             }
            #             nougat_pagess.insert_one(new_book_document)


    except Exception as e:
        print(f"Error: {e}")

@timeit
def get_latext_text(pdf_path, bookname, bookId):
    try:
        command=[
            "nougat",
            pdf_path,
            "--no-skipping"
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except Exception as e:
        print(f"An error occurred while processing {bookname}, page {page_num} with nougat: {str(e)}")

@timeit
def get_nougat_text(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            files = {'file': (f'{pdf_path}', file, 'application/pdf')}
            response = requests.post('http://127.0.0.1:8503/predict/', files=files)
            if response.status_code == 200:
                return 'success'
            else:
                return 'something went worng while processing pdf through nougat'
    except Exception as e:
        print(f"An error occurred while processing pdf with nougat: {str(e)}")


@timeit
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text.lstrip('\\'))
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech

def consume_nougat_queue():
    try:
         # Declare the queue
        channel.queue_declare(queue='nougat_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='nougat_queue', on_message_callback=extract_text_equation_with_nougat)

        print(' [*] Waiting for messages on nougat_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass


extract_text_equation_with_nougat()
# if __name__ == "__main__":
#     try:
#         consume_nougat_queue()     
#     except KeyboardInterrupt:
#         pass
  


