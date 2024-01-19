# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import traceback
import os
from bud_api_table_extract import process_book_page
from pdf_pipeline.pdf_producer import error_queue
import json
from utils import (
    get_mongo_collection,
    get_rabbitmq_connection,
    get_channel
)

QUEUE_NAME = "bud_table_extraction_queue"

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

table_collection = get_mongo_collection('table_collection')
latex_pages = get_mongo_collection('latex_pages')

def extract_page_table(ch, method, properties, body):
    message = json.loads(body)
    tableId = message["tableId"]
    data = message["data"]
    image_path = data["img"]
    bookId = message["bookId"]
    page_num = message["page_num"]

    print(f"""
          received extract table request for 
          book : {bookId}, 
          page number : {page_num},
          table id : {tableId}
    """)
    try:
        existing_table = table_collection.find_one({"bookId": bookId, "tableId": tableId})
        if existing_table:
            return
        table_data = process_book_page(image_path, tableId)
        if table_data:
            table_collection.insert_one({
                "bookId": bookId,
                "tableId": tableId,
                "page_num": page_num,
                "table_data": table_data[0]
            })
        else:
            print(f"Table with id: {tableId} not extracted")
        if table_data is None:
            raise Exception("table not extracted - possibly API error")
        image_path = os.path.abspath(image_path)
        if os.path.exists(image_path):
            os.remove(image_path)
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno
        } 
        error_queue('', bookId, error)    
    finally:
        print("ack sent")
        ch.basic_ack(delivery_tag=method.delivery_tag)
  
def consume_table_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)
    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=extract_page_table)

    print(f' [*] Waiting for messages on {QUEUE_NAME} To exit, press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_table_queue()      
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()