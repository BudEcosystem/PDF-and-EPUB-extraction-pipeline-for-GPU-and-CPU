from dotenv import load_dotenv
import subprocess
import sys
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
from PIL import Image
import os
import requests
import PyPDF2
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

from pdf_producer import book_completion_queue
from nougat.utils.checkpoint import get_checkpoint

from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

# CHECKPOINT = get_checkpoint('nougat')

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
error_collection = db.error_collection
nougat_pages=db.nougat_pages
nougat_done=db.nougat_done

@timeit
def extract_text_equation_with_nougat(ch, method, properties, body):
    try:
        message = json.loads(body)
        pdf_path=message['pdf_path']
        bookname= message['bookname']
        bookId=message['bookId']
        message=get_nougat_text(pdf_path)
        if message=='success':
            print("done")
    except Exception as e:
        print(f'error occured while processing {page_num} of book {bookname} though nougat {str(e)}') 
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)



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
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text.lstrip('\\'))
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech

def consume_nougat_pdf_queue():
    try:
         # Declare the queue
        channel.queue_declare(queue='nougat_pdf_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='nougat_pdf_queue', on_message_callback=extract_text_equation_with_nougat)

        print(' [*] Waiting for messages on nougat_pdf_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    try:
        consume_nougat_pdf_queue()     
    except KeyboardInterrupt:
        pass
  


