# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import sys
import traceback
import shutil
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
load_dotenv()
import os
import json
from datetime import datetime
from pdf_producer import error_queue
from utils import get_mongo_collection, get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

books = get_mongo_collection('book_set_2')
book_details = get_mongo_collection('book_details')
nougat_pages = get_mongo_collection('nougat_pages')
other_pages = get_mongo_collection('other_pages')
latex_pages = get_mongo_collection('latex_pages')

QUEUE_NAME = "book_completion_queue"

@timeit
def book_complete(ch, method, properties, body):
    message = json.loads(body)
    bookId = message["bookId"]
    print(f"Received book completion request for {bookId}")
    
    try:
        book_already_completed = books.find_one({"bookId": bookId})
        if book_already_completed:
            print(f"book {bookId} already extracted")
            return
        book_det = book_details.find_one({"bookId": bookId})
        book_name = book_det["book"]
        book_path = book_det["book_path"]
        num_pages_done = book_det.get("num_pages_done")
        num_pages = book_det.get("num_pages")
        book_completed = False
        if num_pages_done >= num_pages:
            book_completed = True
        if book_completed:
            other_pages_document = other_pages.find_one({"bookId": bookId})
            nougat_pages_document = nougat_pages.find_one({"bookId": bookId})
            latex_pages_document = latex_pages.find_one({"bookId": bookId})

            # Initialize lists to hold pages from each document
            other_pages_result = other_pages_document.get("pages", []) if other_pages_document else []
            nougat_pages_result = []
            nougat_pages_result_dict = nougat_pages_document.get("pages", []) if nougat_pages_document else []
            if nougat_pages_result_dict:
                nougat_pages_result = [result for page_num, result in nougat_pages_result_dict.items()]
            latex_pages_result = latex_pages_document.get("pages", []) if latex_pages_document else []

            all_pages =  other_pages_result + nougat_pages_result +  latex_pages_result
            sorted_pages = sorted(all_pages, key = lambda x: int(x.get("page_num", 0)))
            new_document = {
                "bookId": bookId,
                "book": book_name,
                "pages": sorted_pages,
            }
            books.insert_one(new_document)     
            current_time = datetime.now().strftime("%H:%M:%S")
            book_details.update_one(
                {"bookId": bookId},
                {"$set": {"status": "extracted", "end_time": current_time}},
            )
            book_folder = os.path.dirname(book_path)
            if os.path.exists(book_folder):
                shutil.rmtree(book_folder)  
        else:
            print(f"Book {bookId} not yet completed")
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno
        }
        error_queue(book_name, bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_book_completion_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)

    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=book_complete)

    print(' [*] Waiting for messages on book_completion_queue. To exit, press CTRL+C')
    channel.start_consuming()
    

if __name__ == "__main__":
    try:
        consume_book_completion_queue()      
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()