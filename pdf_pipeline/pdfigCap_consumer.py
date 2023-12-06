import pika
import json
from dotenv import load_dotenv
import sys
import os
import shutil
import GPUtil
import psutil

sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")

from FigCap import extract_figure_and_caption
from utils import timeit
import pymongo
import PyPDF2
import uuid
from pdf_producer import check_ptm_completion_queue
from PyPDF2 import PdfReader
from rabbitmq_connection import get_rabbitmq_connection, get_channel
connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
figure_caption = db.figure_caption

@timeit
def get_figure_and_captions(ch, method, properties, body):
    message = json.loads(body)
    print(message)
    job = message['job']
    book_path = message["pdf_path"]
    bookname = message["bookname"]
    bookId = message["bookId"]

    document = figure_caption.find_one({"bookId":bookId})
    if document:
        return
    someId=uuid.uuid4().hex
    name1='pdffiles'+someId
    name2='output'+someId
    output_directory = os.path.abspath(name1)
    book_output = os.path.abspath(name2)
    os.makedirs(output_directory, exist_ok=True)
    os.makedirs(book_output, exist_ok=True)
    with open(book_path, 'rb') as pdf_file:
        pdf_reader =PyPDF2.PdfReader(pdf_file)
        num_pages = len(pdf_reader.pages)
        pages_per_split = 15
        for i in range(0, num_pages, pages_per_split):
            pdf_writer = PyPDF2.PdfWriter()
            for page_num in range(i, min(i + pages_per_split, num_pages)):
                 page = pdf_reader.pages[page_num]
                 pdf_writer.add_page(page)

            # Save the smaller PDF to the output directory
            output_filename = os.path.join(output_directory, f'output_{i // pages_per_split + 1}.pdf')
            with open(output_filename, 'wb') as output_file:
                pdf_writer.write(output_file)   
    try:
        book_data=extract_figure_and_caption(output_directory, book_output)
        process = psutil.Process(os.getpid())
        print(f"Memory Usage for figure caption function: {process.memory_info().rss / (1024 ** 2):.2f} MB")
        gpus = GPUtil.getGPUs()
        for i, gpu in enumerate(gpus):
            print(f"GPU {i + 1} - GPU Name: {gpu.name}")
            print(f"  GPU Utilization: {gpu.load * 100:.2f}%")
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)   
        if os.path.exists(book_output):
            shutil.rmtree(book_output)
        if book_data:
            figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": book_data,'status':"success"})
            print("Book's figure and figure caption saved in the database")
            check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
        else:
            print(f"no figure detected by pdfigcapx for this book {bookname}")
            figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": [], "status":"failed"})
            check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)   
        if os.path.exists(book_output):
            shutil.rmtree(book_output)
        figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": [], "status":"failed"})
        check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Unable to get figure and figure caption for this {bookname}, {str(e)}, line_number {traceback.extract_tb(e.__traceback__)[-1].lineno}")



def consume_pdfigcap_queue():
    try: 
        # Declare the queue
        channel.queue_declare(queue='pdfigcap_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='pdfigcap_queue', on_message_callback=get_figure_and_captions)

        print(' [*] Waiting for messages on pdfigcap_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass



if __name__ == "__main__":
    try:
        consume_pdfigcap_queue()
        
    except KeyboardInterrupt:
        pass
