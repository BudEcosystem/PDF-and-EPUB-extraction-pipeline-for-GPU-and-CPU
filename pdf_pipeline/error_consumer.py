# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import os
import pymongo
import json
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
error_collection= db.error_collection

def store_errors(ch, method, properties, body):
    try:
        message = json.loads(body)
        bookname = message["bookname"]
        bookId = message["bookId"]
        error=message['error']
        error_doc=error_collection.find_one({"bookId":bookId})
        if error_doc:
            # If the document exists, update the errors array
            error_collection.update_one(
                {'bookId': bookId},
                {'$push': {'errors': error}}
            )
            print(f"Updated existing document for bookId: {bookId}")
        else:
            # If the document does not exist, create a new one
            new_document = {
                'book': bookname,
                'bookId': bookId,
                'errors': [error]
            }
            error_collection.insert_one(new_document)

    except Exception as e:
        print("error while storing erros", e)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
   

def consume_error_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='error_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='error_queue', on_message_callback=store_errors)

        print(' [*] Waiting for messages on error_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()

   


if __name__ == "__main__":
    try:
        consume_error_queue()      
    except KeyboardInterrupt:
        pass