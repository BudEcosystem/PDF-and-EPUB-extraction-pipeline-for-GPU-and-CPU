from dotenv import load_dotenv
import pymongo
import sys
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
load_dotenv()
import os
import pika
import json
from datetime import datetime

import shutil
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
bookdata = db.book_set_2_new
book_details = db.book_details
error_collection = db.error_collection
nougat_pages=db.nougat_pages
book_other_pages=db.book_other_pages
nougat_done=db.nougat_done
book_other_pages_done=db.book_other_pages_done
bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']

@timeit
def book_complete(ch, method, properties, body):
    try:
        message = json.loads(body)
        job = message['job']
        bookname = message["bookname"]
        bookId = message["bookId"]
        print(bookId)
        other_pages = book_other_pages_done.find_one({"bookId": bookId})
        nougat_pages_done = nougat_done.find_one({"bookId": bookId})
        if other_pages  and nougat_pages_done:
            book_pages_document = book_other_pages.find_one({"bookId": bookId})
            nougat_pages_document = nougat_pages.find_one({"bookId": bookId})
            if book_pages_document  and nougat_pages_document:
                all_pages = book_pages_document["pages"] + nougat_pages_document["pages"]
                sorted_pages = sorted(all_pages, key=lambda x: x["page_num"])
                new_document = {
                    "bookId": bookId,
                    "book": book_pages_document["book"],
                    "pages": sorted_pages
                }
                bookdata.insert_one(new_document)
                current_time = datetime.now().strftime("%H:%M:%S")
                book_details.update_one({"bookId": bookId}, {"$set": {"status": "extracted","end_time": current_time}})
                book_folder=bookname.split(".")[0]
                # if os.path.exists(book_folder):
                #     shutil.rmtree(book_folder)
                # book_path = os.path.join(folder_name, f'{bookname}')
                # if os.path.exists(book_path):
                #     os.remove(book_path)            
        else:
            print("Not yet completed")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        # Log the error or perform any necessary actions

    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_book_completion_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='book_completion_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='book_completion_queue', on_message_callback=book_complete)

        print(' [*] Waiting for messages on book_completion_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass



if __name__ == "__main__":
    try:
        consume_book_completion_queue()      
    except KeyboardInterrupt:
        pass