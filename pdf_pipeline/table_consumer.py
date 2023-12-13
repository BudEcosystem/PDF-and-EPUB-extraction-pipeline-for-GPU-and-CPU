# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import traceback
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
from PIL import Image
import os
import boto3
import re
import base64
import pymongo
import uuid
from tablecaption import process_book_page
from pdf_producer import error_queue
import json
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()


client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.book_set_2

book_other_pages=db.book_other_pages
book_other_pages_done=db.book_other_pages_done
table_collection=db.table_collection

def extract_page_table(ch, method, properties, body):
    try:
        message = json.loads(body)
        tableId=message['tableId']
        data=message['data']
        image_data_base64 = data['img']
        bookname = message["bookname"]
        bookId = message["bookId"]
        page_num=message['page_num']
        print(page_num)
        # Decode base64 and save the image
        image_data = base64.b64decode(image_data_base64)
        with open('received_image.jpg', 'wb') as received_image:
            received_image.write(image_data)
        table_data=process_book_page('received_image.jpg', tableId)
        if table_data:
            page_details={
                "page_num":page_num,
                "page_tables":[table_data]
            }
            existing_doc = table_collection.find_one({"bookId": bookId})
            if existing_doc:
                for existing_page in existing_doc["pages"]:
                    if existing_page["page_num"] == page_num:
                        existing_page["page_tables"].extend(table_data)
                        break
                else:
                    # Page with the same page_num doesn't exist, append the new page_details
                    table_collection.update_one({"bookId": bookId},{"$push": {"pages": page_details}})
            else:
                # Document doesn't exist, create a new one
                table_doc = {
                    "bookId": bookId,
                    "book": bookname,
                    "pages": [page_details]
                }
                table_collection.insert_one(table_doc)
        image_path=os.path.abspath('received_image.jpg')
        if os.path.exists(image_path):
            os.remove(image_path)
    except Exception as e:
        error = {"consumer":"table_consumer","consumer_message":message, "error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue',bookname, bookId,error)    
    finally:
        print("ack received")
        ch.basic_ack(delivery_tag=method.delivery_tag)
  
def consume_table_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='table_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='table_queue', on_message_callback=extract_page_table)

        print(' [*] Waiting for messages on stable_queue To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()

   


if __name__ == "__main__":
    try:
        consume_table_queue()      
    except KeyboardInterrupt:
        pass