from dotenv import load_dotenv
import subprocess
import sys
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
from PIL import Image
import os
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

CHECKPOINT = get_checkpoint('nougat')

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
error_collection = db.error_collection
nougat_pages=db.nougat_pages
nougat_done=db.nougat_done

@timeit
def extract_text_equation_with_nougat(ch, method, properties, body):
    message = json.loads(body)
    image_path=message['image_path']
    total_nougat_pages=message['total_nougat_pages']
    book_page_num=message['book_page_num']
    page_num=message['page_num']
    bookname= message['bookname']
    bookId=message['bookId']
    job = message['job']
    
    page_equations=[]
    pdf_file_name ="page.pdf"
    pdf_path = os.path.abspath(pdf_file_name)

    with open(pdf_path, "wb") as pdf_file, open(image_path, "rb") as image_file:
        pdf_file.write(img2pdf.convert(image_file))
    latex_text=get_latext_text(pdf_path,bookname, bookId)
    latex_text = latex_text.replace("[MISSING_PAGE_EMPTY:1]", "")
    if latex_text == "":
        latex_text = ""
    pattern = r'(\\\(.*?\\\)|\\\[.*?\\\])'
    
    def replace_with_uuid(match):
        equationId = uuid.uuid4().hex
        match_text = match.group()
        text_to_speech=latext_to_text_to_speech(match_text)
        page_equations.append({'id': equationId, 'text': match_text, 'text_to_speech':text_to_speech})
        return f'{{{{equation:{equationId}}}}}'
    
    page_content = re.sub(pattern, replace_with_uuid, latex_text)
    page_content = re.sub(r'\s+', ' ', page_content).strip()

    page_object={
        "page_num":book_page_num,
        "text":page_content,
        "tables":[],
        "figures":[],
        "page_equations":page_equations
    }
    book_document = nougat_pages.find_one({"bookId":  bookId})
    if book_document:
        nougat_pages.update_one({"_id": book_document["_id"]}, {"$push": {"pages": page_object}})
    else:
        new_book_document = {
            "bookId": bookId,
            "book": bookname,  
            "pages": [page_object]
            }
        nougat_pages.insert_one(new_book_document)
    if os.path.exists(pdf_path):
        os.remove(pdf_path)
    if total_nougat_pages==page_num+1:
        nougat_done.insert_one({"bookId":bookId,"book":bookname,"status":"nougat pages Done"})
        book_completion_queue("book_completion_queue",bookname, bookId)
    ch.basic_ack(delivery_tag=method.delivery_tag)  

@timeit
def get_latext_text(pdf_path, bookname, bookId):
    try:
        command=[
            "nougat",
            pdf_path,
            '--checkpoint', CHECKPOINT,
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

if __name__ == "__main__":
    try:
        consume_nougat_queue()     
    except KeyboardInterrupt:
        pass
  


