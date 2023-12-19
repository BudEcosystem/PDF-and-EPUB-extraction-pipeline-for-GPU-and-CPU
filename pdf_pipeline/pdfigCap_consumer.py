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
from code.FigCap import extract_figure_and_caption
from utils import timeit, get_mongo_collection, generate_unique_id
from pdf_producer import send_to_queue, error_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel


load_dotenv()

figure_caption = get_mongo_collection('figure_caption')

class DocumentFound(Exception):
    pass

@timeit
def get_figure_and_captions(ch, method, properties, body):
    message = json.loads(body)
    book_path = message["book_path"]
    bookId = message["bookId"]
    from_page = message["from_page"]
    to_page = message["to_page"]
    print(f"Received message for {bookId}")
    queue_msg = {
        "bookId": bookId,
        "split_path": book_path
    }
    try:
        document = figure_caption.find_one({
            "bookId" : bookId,
            "split_path": book_path})
        if document:
            raise DocumentFound("Figure Caption Book already exist in the database")
    except DocumentFound as e:
        print(e)
        send_to_queue('check_ptm_completion_queue', queue_msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    pdf_input_folder = generate_unique_id()
    os.makedirs(pdf_input_folder, exist_ok=True)
    output_folder = f"{pdf_input_folder}_output"
    os.makedirs(output_folder, exist_ok=True)
    shutil.copy(book_path, pdf_input_folder)
    try:
        book_data = extract_figure_and_caption(pdf_input_folder, output_folder)
        if book_data:
            figure_caption.insert_one({
                "bookId": bookId,
                "split_path": book_path,
                "pages": book_data,
                "status": "success",
                "from_page": from_page,
                "to_page": to_page
            })
            print(f"Book's {book_path} figure saved")
            send_to_queue('check_ptm_completion_queue', queue_msg)
        else:
            print(f"no figure detected by pdfigcapx for this book {book_path}")
            figure_caption.insert_one({
                "bookId": bookId,
                "split_path": book_path,
                "pages": [],
                "status":"failed",
                "from_page": from_page,
                "to_page": to_page
            })
            send_to_queue('check_ptm_completion_queue', queue_msg)
    except Exception as e:
        figure_caption.insert_one({
            "bookId": bookId,
            "split_path": book_path,
            "pages": [],
            "status":"failed",
            "from_page": from_page,
            "to_page": to_page
        })
        error = {
            "consumer": "pdfigcapx",
            "consumer_message": message,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno
        } 
        print(error)
        send_to_queue('check_ptm_completion_queue', queue_msg)
        error_queue('error_queue', book_path, bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if os.path.exists(pdf_input_folder):
            shutil.rmtree(pdf_input_folder)   
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)


def consume_pdfigcap_queue():
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
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
    finally:
        connection.close()


if __name__ == "__main__":
    consume_pdfigcap_queue()     
