# pylint: disable=all
# type: ignore
import pika.exceptions
import json
from dotenv import load_dotenv
import sys
import os
import shutil
import traceback
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
from FigCap import extract_figure_and_caption
from utils import timeit, get_mongo_collection
import PyPDF2
import uuid
from pdf_producer import check_ptm_completion_queue, error_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

figure_caption = get_mongo_collection('figure_caption')

class DocumentFound(Exception):
    pass

@timeit
def get_figure_and_captions(ch, method, properties, body):
    message = json.loads(body)
    print(message)
    book_path = message["pdf_path"]
    bookname = message["bookname"]
    bookId = message["bookId"]
    try:
        document = figure_caption.find_one({"bookId":bookId})
        if document:
            raise DocumentFound("Figure Caption Book already exist in the database")
    except DocumentFound as e:
        print(e)
        check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
        ch.basic_ack(delivery_tag=method.delivery_tag)
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
            count = i
            for page_num in range(i, min(i + pages_per_split, num_pages)):
                 page = pdf_reader.pages[page_num]
                 pdf_writer.add_page(page)
                 count += 1

            # Save the smaller PDF to the output directory
            output_filename = os.path.join(output_directory, f'output_{i+1}-{count}_{i // pages_per_split + 1}.pdf')
            with open(output_filename, 'wb') as output_file:
                pdf_writer.write(output_file) 
    try:
        book_data=extract_figure_and_caption(output_directory, book_output)
        if book_data:
            figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": book_data,'status':"success"})
            print("Book's figure and figure caption saved in the database")
            check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
            print("hello jghfgfg")
        else:
            print(f"no figure detected by pdfigcapx for this book {bookname}")
            figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": [], "status":"failed"})
            check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
        print("hello world ")
    except Exception as e:
        figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": [], "status":"failed"})
        error ={"consumer":"pdfigcap","consumer_message":message, "error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
        error_queue('error_queue',bookname, bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("gfhdtg")
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)   
        if os.path.exists(book_output):
            shutil.rmtree(book_output)


def consume_pdfigcap_queue():
    try: 

        channel.basic_qos(prefetch_count=1, global_qos=False)
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
