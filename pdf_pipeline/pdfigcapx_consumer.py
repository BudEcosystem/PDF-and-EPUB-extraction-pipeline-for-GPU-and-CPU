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
from utils import (
    timeit,
    get_mongo_collection,
    generate_unique_id,
    get_rabbitmq_connection,
    get_channel
)
from pdf_producer import send_to_queue, error_queue


load_dotenv()

figure_caption = get_mongo_collection('figure_caption')

class DocumentFound(Exception):
    pass

def transform_to_figure_blocks(book_data):
    figure_layout = []
    # sonali : how did we decide on these values ?
    # and if constant value then move to .env or global variable
    old_page_width = 439
    old_page_height = 666
    new_page_width = 1831
    new_page_height = 2776
    for page in book_data:
        figure_bbox_values = page.get("figure_bbox")
        caption_text = page.get('caption_text')
        caption = ''.join(caption_text)

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
        figure_layout.append(figure_block)
    return figure_layout

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
            figure_layout = transform_to_figure_blocks(book_data)
            figure_caption.insert_one({
                "bookId": bookId,
                "split_path": book_path,
                "pages": figure_layout,
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
