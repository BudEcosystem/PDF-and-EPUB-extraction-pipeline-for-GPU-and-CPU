# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import traceback
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import os
import pymongo
import json
from pdf_producer import page_extraction_queue, error_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
figure_caption = db.figure_caption
table_bank_done=db.table_bank_done
publaynet_done=db.publaynet_done
mfd_done=db.mfd_done
publaynet_book_job_details=db.publaynet_book_job_details
table_bank_book_job_details=db.table_bank_book_job_details
mfd_book_job_details=db.mfd_book_job_details

def check_ptm_status(ch, method, properties, body):
    try:
        print("hello pmt called")
        message = json.loads(body)
        bookname = message["bookname"]
        bookId = message["bookId"]

        publeynet_done_document = publaynet_done.find_one({"bookId": bookId})
        table_done_document = table_bank_done.find_one({"bookId": bookId})
        mfd_done_document = mfd_done.find_one({"bookId": bookId})
        pdFigCap = figure_caption.find_one({"bookId": bookId})

        if publeynet_done_document and table_done_document and mfd_done_document and pdFigCap:
            collections = [publaynet_book_job_details, table_bank_book_job_details, mfd_book_job_details]
            page_results = {}

            for collection in collections:
                documents = collection.find({"bookId": bookId})  # Use find() to get multiple documents
                for document in documents:
                    for page in document.get("pages", []):
                        page_num = page["page_num"]
                        result_array = page.get("result", [])
                        image_path = page.get("image_path", "")
                        for result in result_array:
                            result["image_path"] = image_path

                        # Initialize an empty list for the page if it doesn't exist in the dictionary
                        if page_num not in page_results:
                            page_results[page_num] = []
                        page_results[page_num].extend(result_array)

            for page_num, result_array in page_results.items():
                if not result_array:
                    print(page_num)
                    print(bookId)
                    # Fetch image_path from publeynet_collection for the given page_num
                    publeynet_document = publaynet_book_job_details.find_one({"bookId": bookId})
                    for page in publeynet_document['pages']:
                        if page['page_num']==page_num:
                            image_path=page['image_path']
                            page_results[page_num].append({"image_path": image_path})

            page_extraction_queue('page_extraction_queue', page_results, bookname,bookId)
        else:
            print("not yet completed")
    except Exception as e:
        error = {"consumer":"check_ptm_consumer","error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue',bookname, bookId,error)      
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_ptm_completion_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='check_ptm_completion_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='check_ptm_completion_queue', on_message_callback=check_ptm_status)

        print(' [*] Waiting for messages on check_ptm_completion_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()
   


if __name__ == "__main__":
    try:
        consume_ptm_completion_queue()      
    except KeyboardInterrupt:
        pass