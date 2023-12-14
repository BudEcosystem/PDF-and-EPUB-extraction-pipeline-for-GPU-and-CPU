# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import sys
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
import os
import requests
import img2pdf
import traceback
import json


from rabbitmq_connection import get_rabbitmq_connection, get_channel
from pdf_producer import book_completion_queue, error_queue
from utils import get_mongo_collection, create_image_from_str

connection = get_rabbitmq_connection()
channel = get_channel(connection)
channel.basic_qos(prefetch_count=1, global_qos=False)

load_dotenv()

# NOUGAT_API_URL=os.environ['NOUGAT_API_URL']

nougat_done=get_mongo_collection('nougat_done')

@timeit
def extract_text_equation_with_nougat(ch, method, properties, body):
    try:
        message = json.loads(body)
        results = message['results']
        bookname = message['bookname']
        bookId = message['bookId']
        print(f"nougat received message for {bookname} ({bookId})")
        nougat_pages_doc = nougat_done.find_one({"bookId": bookId})
        if nougat_pages_doc:
            book_completion_queue('book_completion_queue', bookname, bookId)
            return
        pdf_file_name = f"{bookId}.pdf"
        pdf_path = os.path.abspath(pdf_file_name)

        # sonali: create pdf from image strings
        image_strs = [result['image_str'] for result in results]
        image_paths = []
        for image_str in image_strs:
            image_paths.append(create_image_from_str(image_str))

        # Iterate over results and create PDF
        # image_paths = [result['image_path'] for result in results]
        with open(pdf_path, "wb") as f_pdf:
            f_pdf.write(img2pdf.convert(image_paths))
        
        for img_path in image_paths:
            os.remove(img_path)

        page_nums = [result['page_num'] for result in results]
        api_data = {
            "bookId": bookId,
            "bookname": bookname,
            "page_nums": page_nums
        }
        _ = get_nougat_extraction(pdf_path, api_data)
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        book_completion_queue('book_completion_queue', bookname, bookId)
        print("after finish")
    except Exception as e:
        error = {
            "consumer":"nougat_consumer",
            "consumer_message":message,
            "error":str(e),
            "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno
        } 
        print(error)
        error_queue('error_queue', bookname, bookId, error)
    finally:
        print("message ack")
        ch.basic_ack(delivery_tag=method.delivery_tag)



@timeit
def get_nougat_extraction(pdf_path, data):
    files = {'file': (pdf_path, open(pdf_path, 'rb'))}
    response = requests.post(nougat_api_url, files=files, data=data, timeout=None)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None

def consume_nougat_pdf_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)
        queue_name = "nougat_pdf_queue"
         # Declare the queue
        channel.queue_declare(queue=queue_name)
        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue=queue_name, on_message_callback=extract_text_equation_with_nougat)

        print(f' [*] Waiting for messages on {queue_name}. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()


if __name__ == "__main__":
    try:
        nougat_api_url = sys.argv[1]
        consume_nougat_pdf_queue()    
    except KeyboardInterrupt:
        pass
  


