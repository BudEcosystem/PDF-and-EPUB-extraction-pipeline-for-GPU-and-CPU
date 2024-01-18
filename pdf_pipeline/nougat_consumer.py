# pylint: disable=all
# type: ignore
import sys
from utils import timeit
import os
import requests
import img2pdf
import traceback
import json
from dotenv import load_dotenv
from pdf_pipeline.pdf_producer import send_to_queue, error_queue
from utils import (
    get_mongo_collection,
    generate_unique_id,
    get_rabbitmq_connection,
    get_channel
)

load_dotenv()

QUEUE_NAME = 'nougat_queue'

connection = get_rabbitmq_connection()
channel = get_channel(connection)

NOUGAT_BATCH_SIZE = int(os.environ["NOUGAT_BATCH_SIZE"])

book_details = get_mongo_collection('book_details')
nougat_pages_collection = get_mongo_collection('nougat_pages')

@timeit
def extract_text_equation_with_nougat(ch, method, properties, body):
    message = json.loads(body)
    bookId = message['bookId']
    page_num = message.get('page_num', None)
    image_path = message.get('image_path', None)
    split_id = message.get('split_id', None)
    print(f"nougat received message for {bookId}")
    try:
        process_remaining_pages = False
        book = book_details.find_one({"bookId": bookId})
        if book["status"] == "extracted":
            return
        nougat_splits = book['nougat_splits']

        if not split_id:
            split_ids = sorted(map(int, nougat_splits.keys()))
            split_id = str(split_ids[-1])
            process_remaining_pages = True
       
        nougat_pages = nougat_splits[split_id]
        page_present_in_split = True
        if page_num:
            page_present_in_split = any(page['page_num'] == page_num for page in nougat_pages)
        if len(nougat_pages) >= NOUGAT_BATCH_SIZE or process_remaining_pages or \
            not page_present_in_split:
            if split_id in book.get('processing_nougat_split', []) and page_present_in_split:
                return
            if page_present_in_split:
                book_details.update_one(
                    {"bookId": bookId},
                    {"$addToSet": {"processing_nougat_split": split_id}}
                )
            if not page_present_in_split:
                image_paths = [image_path]
            else:
                image_paths = [page["image_path"] for page in nougat_pages]
            # local_image_paths = []
            # for img_path in image_paths:
            #     # will return image_str form db
            #     image_str = generate_image_str(bookId, img_path)
            #     local_image_paths.append(create_image_from_str(image_str))
            #     # pages_extracted += 1

            pdf_path = os.path.abspath(f"{generate_unique_id()}.pdf")
            with open(pdf_path, "wb") as f_pdf:
                # f_pdf.write(img2pdf.convert(local_image_paths))
                f_pdf.write(img2pdf.convert(image_paths))

            # clean up local images created for pdf creation
            # for img_path in local_image_paths:
            #     abs_path = os.path.abspath(img_path)
            #     os.remove(abs_path)

            if not page_present_in_split:
                page_nums = [page_num]
            else:
                page_nums = [page["page_num"] for page in nougat_pages]
            api_data = {
                "bookId": bookId,
                "page_nums": page_nums
            }
            _ = get_nougat_extraction(pdf_path, api_data)
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            n_pages = nougat_pages_collection.find_one({"bookId": bookId})
            pages = n_pages.get('pages', {})
            if pages:
                pages_extracted = len(list(pages.keys()))
                book_details.find_one_and_update(
                    {"bookId": bookId},
                    {
                        "$set": {"num_nougat_pages_done": pages_extracted}
                    }
                )
            send_to_queue('book_completion_queue', bookId)
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
        print("message ack")
        ch.basic_ack(delivery_tag=method.delivery_tag)



@timeit
def get_nougat_extraction(pdf_path, data):
    global nougat_api_url
    files = {'file': (pdf_path, open(pdf_path, 'rb'))}
    response = requests.post(nougat_api_url, files=files, data=data, timeout=None)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception("Out of memory")

def consume_nougat_pdf_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)
    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME)
    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=extract_text_equation_with_nougat)

    print(f' [*] Waiting for messages on {QUEUE_NAME}. To exit, press CTRL+C')
    channel.start_consuming()

    


if __name__ == "__main__":
    try:
        nougat_api_url = sys.argv[1]
        print(f"Nougat consumer connected to {nougat_api_url}")
        consume_nougat_pdf_queue() 
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()
  


