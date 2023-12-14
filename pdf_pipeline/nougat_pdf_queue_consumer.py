# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import subprocess
import sys
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
import os
import GPUtil
import requests
import shutil
import psutil
import img2pdf
import traceback
import re
import pymongo
import uuid
from latext import latex_to_text
import json


from rabbitmq_connection import get_rabbitmq_connection, get_channel
from pdf_producer import book_completion_queue, error_queue

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.book_set_2

error_collection = db.error_collection
nougat_pages=db.nougat_pages
nougat_done=db.nougat_done

@timeit
def extract_text_equation_with_nougat(ch, method, properties, body):
    try:
        message = json.loads(body)
        results=message['results']
        bookname= message['bookname']
        bookId=message['bookId']
        nougat_pages_doc = nougat_done.find_one({"bookId": bookId})
        if nougat_pages_doc:
            book_completion_queue('book_completion_queue', bookname, bookId)
            return
        pdf_file_name = f"{bookId}.pdf"
        pdf_path = os.path.abspath(pdf_file_name)

        # Iterate over results and create PDF
        image_paths = [result['image_path'] for result in results]
        with open(pdf_path, "wb") as f_pdf:
            f_pdf.write(img2pdf.convert(image_paths))
        
        page_nums = [result['page_num'] for result in results]
        pdf_folder_id = get_nougat_extraction(pdf_path)
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        extrcated_pdf_directory = 'pdfs'
        pdfId_path = os.path.join(extrcated_pdf_directory, pdf_folder_id, 'pages')
        if os.path.exists(pdfId_path):
            files = sorted(os.listdir(pdfId_path))
            for filename, page_num in zip(files, page_nums):
                page_equations = []
                file_path = os.path.join(pdfId_path, filename)
                if os.path.isfile(file_path):
                    with open(file_path, 'r', encoding='utf-8') as file:
                        latex_text = file.read()

                latex_text = latex_text.replace("[MISSING_PAGE_POST]", "")
                if latex_text == "":
                    latex_text = ""
                pattern = r'(\\\(.*?\\\)|\\\[.*?\\\])'
                def replace_with_uuid(match):
                    equationId = uuid.uuid4().hex
                    match_text = match.group()
                    text_to_speech = latext_to_text_to_speech(match_text)
                    page_equations.append({'id': equationId, 'text': match_text, 'text_to_speech': text_to_speech})
                    return f'{{{{equation:{equationId}}}}}'
                page_content = re.sub(pattern, replace_with_uuid, latex_text)
                page_content = re.sub(r'\s+', ' ', page_content).strip()
                page_object = {
                    "page_num": page_num,
                    "text": page_content,
                    "tables": [],
                    "figures": [],
                    "page_equations": page_equations
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
            nougat_done.insert_one({"bookId": bookId, "book": bookname, "status": "nougat pages Done"})
            shutil.rmtree(os.path.join(extrcated_pdf_directory, pdf_folder_id))
            book_completion_queue('book_completion_queue', bookname, bookId)
            
            print("after finish")
    except Exception as e:
        error = {"consumer":"nougat_consumer","consumer_message":message, "error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue',bookname, bookId, error)
    finally:
        print("message ack")
        ch.basic_ack(delivery_tag=method.delivery_tag)



@timeit
def get_nougat_extraction(pdf_path):
    files = {'file': (pdf_path, open(pdf_path, 'rb'))}
    response = requests.post('http://127.0.0.1:8503/predict', files=files)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None

@timeit
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text.lstrip('\\'))
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech

def consume_nougat_pdf_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)

         # Declare the queue
        channel.queue_declare(queue='nougat_pdf_queue')
        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='nougat_pdf_queue', on_message_callback=extract_text_equation_with_nougat)

        print(' [*] Waiting for messages on nougat_pdf_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()


if __name__ == "__main__":
    try:
        consume_nougat_pdf_queue()    
    except KeyboardInterrupt:
        pass
  


