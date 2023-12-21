# pylint: disable=all
# type: ignore
import json
import sys
import os
import traceback
from PyPDF2 import PdfReader
import fitz
from datetime import datetime

sys.path.append("pdf_extraction_pipeline")
from utils import (
    timeit,
    get_mongo_collection,
    split_pdf,
    download_book_from_aws,
    generate_image_str,
    find_split_path,
    get_page_num_from_split_path,
    get_rabbitmq_connection,
    get_channel
)
from pdf_producer import send_to_queue, error_queue


connection = get_rabbitmq_connection()
channel = get_channel(connection)

book_details = get_mongo_collection('book_details')
figure_caption = get_mongo_collection('figure_caption')


@timeit
def process_book(ch, method, properties, body): 
    message = json.loads(body)
    book = message["book"]
    book_name = book['book']
    book_id = book['bookId']
    try:
        book_data = book_details.find_one({'bookId': book_id})
        if book_data and book_data['status'] == 'processing':
            raise Exception("Book is already being processed")
        
        current_time = datetime.now().strftime("%H:%M:%S")
        print(current_time)
        book_details.update_one(
            {'bookId': book_id},
            {'$set': {'status': 'processing', 'start_time': current_time}}
        )
        
        book_path = None
        if book_data:
            book_path = book_data.get('book_path', None)
        if not book_path:
            book_path = download_book_from_aws(book_id, book_name)
            if not book_path:
                raise Exception("Book not found")
            book_details.update_one(
                {'bookId': book_id},
                {'$set': {'book_path': book_path}}
            )

        # split book into batches
        split_book_paths = []
        if book_data:
            split_book_paths = book_data.get('split_book_paths', [])
            num_pages = book_data.get('num_pages', None)
        if not split_book_paths:
            book = PdfReader(book_path)  
            num_pages = len(book.pages)
            split_book_paths = split_pdf(book_id, book_path)
            # save in book_details
            book_details.update_one(
                {'bookId': book_id},
                {'$set': {'split_book_paths': split_book_paths,
                        'num_pages': num_pages
                }}
            )
        book_folder = os.path.join(*book_path.split("/")[:-1])
        print(f"{book_name} has total {num_pages} page")

        if len(split_book_paths) > 1:
            queue_data = {"bookId": book_id}
            for split_path in split_book_paths:
                _, _, from_page, to_page = get_page_num_from_split_path(split_path)
                queue_data["split_path"] = split_path
                queue_data["from_page"] = from_page
                queue_data["to_page"] = to_page
                send_to_queue('pdfigcapx_queue', queue_data)
        else:
            split_path = split_book_paths[0]
            _, _, from_page, to_page = get_page_num_from_split_path(split_path)
            figure_caption.insert_one({
                "bookId": book_id,
                "split_path": split_path,
                "pages": [],
                "status": "failed",
                "from_page": from_page,
                "to_page": to_page
            })
        
        pdf_book = fitz.open(book_path)
        for page_num in range(1, num_pages + 1):
            split_path = find_split_path(split_book_paths, page_num)
            process_page(page_num, pdf_book, book_folder, split_path)
    except Exception as e:
        error = {
            "consumer" : "pdf_consumer",
            "consumer_message" : message,
            "error" : str(e),
            "line_number" : traceback.extract_tb(e.__traceback__)[-1].lineno
        }
        print(error) 
        error_queue('error_queue', book_name, book_id, error)   
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

  
@timeit
def process_page(page_num, pdf_book, book_folder, split_path):
    book_id = book_folder.split('/')[-1]
    # pdf_book is zero indexed, therefore subtract 1
    page_image = pdf_book[page_num - 1]
    page_images_folder_path = os.path.join(book_folder, 'pages')
    os.makedirs(page_images_folder_path, exist_ok=True)
    book_image = page_image.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
    image_path = os.path.join(page_images_folder_path, f'page_{page_num}.jpg')
    book_image.save(image_path)
    absolute_image_path = os.path.abspath(image_path)
    queue_msg = {
        "page_num": page_num,
        "bookId": book_id,
        "split_path": split_path,
        "image_path": absolute_image_path,
        "image_str": generate_image_str(absolute_image_path)
    }
    send_to_queue('publaynet_queue', queue_msg)
    send_to_queue('table_bank_queue', queue_msg)
    send_to_queue('mfd_queue', queue_msg)


def consume_pdf_processing_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)
        # Declare the queue
        channel.queue_declare(queue='pdf_processing_queue')
        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='pdf_processing_queue', on_message_callback=process_book)

        print(' [*] Waiting for messages on pdf_processing_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


if __name__ == "__main__":
    consume_pdf_processing_queue()
