# pylint: disable=all
# type: ignore
# import sys
# sys.path.append("pdf_extraction_pipeline/code")
# sys.path.append("pdf_extraction_pipeline")
import json
from utils import get_mongo_collection, get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

error_collection = get_mongo_collection('error_collection')

def store_errors(ch, method, properties, body):
    try:
        message = json.loads(body)
        bookId = message["bookId"]
        error = message["error"]
        error_doc = error_collection.find_one({"bookId": bookId})
        if error_doc:
            # If the document exists, update the errors array
            error_collection.update_one(
                {'bookId': bookId},
                {'$push': {'errors': error}}
            )
            print(f"Updated error to existing document for bookId: {bookId}")
        else:
            # If the document does not exist, create a new one
            new_document = {
                'bookId': bookId,
                'errors': [error]
            }
            error_collection.insert_one(new_document)
            print(f"Add error document for bookId: {bookId}")
    except Exception as e:
        print("error while storing erros", e)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
   

def consume_error_queue():
    queue_name = "error_queue"
    channel.basic_qos(prefetch_count=1, global_qos=False)

    # Declare the queue
    channel.queue_declare(queue=queue_name)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=queue_name, on_message_callback=store_errors)

    print(' [*] Waiting for messages on error_queue. To exit, press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_error_queue()      
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()