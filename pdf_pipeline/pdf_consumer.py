# pylint: disable=all
# type: ignore
import json
import os
import traceback
from PyPDF2 import PdfReader
import fitz
from datetime import datetime

from utils import (
    timeit,
    get_mongo_collection,
    download_book_from_aws,
    get_rabbitmq_connection,
    get_channel,
)
from pdf_pipeline.pdf_producer import send_to_queue, error_queue

QUEUE_NAME = "pdf_processing_queue"

connection = get_rabbitmq_connection()
channel = get_channel(connection)

book_details = get_mongo_collection("book_details")
figure_caption = get_mongo_collection("figure_caption")

PTM_BATCH_SIZE = int(os.environ["PTM_BATCH_SIZE"])


@timeit
def process_book(ch, method, properties, body):
    message = json.loads(body)
    book = message["book"]
    book_name = book["book"]
    book_id = book["bookId"]
    try:
        book_data = book_details.find_one({"bookId": book_id})
        if book_data and book_data["status"] == "processing":
            raise Exception("Book is already being processed")

        current_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        print(current_time)
        book_details.update_one(
            {"bookId": book_id},
            {"$set": {"status": "processing", "start_time": current_time}},
        )

        book_path = None
        if book_data:
            book_path = book_data.get("book_path", None)
        if not book_path:
            book_path = download_book_from_aws(book_id, book_name)
            if not book_path:
                raise Exception("Book not found")
            book_details.update_one(
                {"bookId": book_id}, {"$set": {"book_path": book_path}}
            )

        # split book into batches
        # split_book_paths = []
        num_pages = None
        if book_data:
            # split_book_paths = book_data.get("split_book_paths", [])
            num_pages = book_data.get("num_pages", None)
        # if not split_book_paths:
        if not num_pages:
            try:
                book = PdfReader(book_path)
            except Exception as e:
                print("error while reading pdf file", e)
                error = {
                    "consumer": QUEUE_NAME,
                    "consumer_message": message,
                    "error": str(e),
                    "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno,
                }
                error_queue(book_name, book_id, error)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            num_pages = len(book.pages)
            # split_book_paths = split_pdf(book_id, book_path)
            # save in book_details
            book_details.update_one(
                {"bookId": book_id},
                {
                    "$set": {
                        # "split_book_paths": split_book_paths,
                        "num_pages": num_pages,
                        # "check_accuracy": True
                    }
                },
            )

        book_folder = os.path.join(*book_path.split("/")[:-1])
        print(f"{book_name} has total {num_pages} page")
        ch.basic_ack(delivery_tag=method.delivery_tag)

        pdf_book = fitz.open(book_path)
        # for page_num in range(1, num_pages + 1):
        #     # split_path = find_split_path(split_book_paths, page_num)
        #     # process_page(page_num, pdf_book, book_folder, split_path)
        #     process_page(page_num, pdf_book, book_folder)

        # book_id = book_folder.split("/")[-1]
        page_images_folder_path = os.path.join(
            book_folder, "pages", book_path.split("/")[-1].replace(".", "_")
        )
        os.makedirs(page_images_folder_path, exist_ok=True)
        for i in range(0, num_pages, PTM_BATCH_SIZE):
            page_nums = list(range(i + 1, i + PTM_BATCH_SIZE + 1))
            image_paths, page_numbers = process_pages(
                page_nums,
                pdf_book.pages(i, i + PTM_BATCH_SIZE),
                page_images_folder_path,
            )
            queue_msg = {
                "page_num": page_numbers,
                "bookId": book_id,
                "image_path": image_paths,
            }
            send_to_queue("ptm_queue", queue_msg)
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno,
        }
        error_queue(book_name, book_id, error)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    # finally:
    #     ch.basic_ack(delivery_tag=method.delivery_tag)


def process_pages(page_nums, pdf_book, page_images_folder_path):
    image_paths = []
    page_numbers = []
    for i, pdf_page in enumerate(pdf_book):
        page_num = page_nums[i]
        page_numbers.append(page_num)
        img_path = os.path.join(page_images_folder_path, f"page_{page_num}.png")
        book_image = pdf_page.get_pixmap(dpi=300)
        if not os.path.exists(img_path):
            book_image.pil_save(img_path)
        absolute_image_path = os.path.abspath(img_path)
        image_paths.append(absolute_image_path)
    return image_paths, page_numbers


@timeit
# def process_page(page_num, pdf_book, book_folder, split_path):
def process_page(page_num, pdf_book, book_folder):
    book_id = book_folder.split("/")[-1]
    # pdf_book is zero indexed, therefore subtract 1
    page_image = pdf_book[page_num - 1]
    page_images_folder_path = os.path.join(book_folder, "pages")
    os.makedirs(page_images_folder_path, exist_ok=True)
    book_image = page_image.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
    # book_image = page_image.get_pixmap()
    image_path = os.path.join(page_images_folder_path, f"page_{page_num}.jpg")
    if not os.path.exists(image_path):
        book_image.save(image_path)
    absolute_image_path = os.path.abspath(image_path)
    queue_msg = {
        "page_num": page_num,
        "bookId": book_id,
        "image_path": absolute_image_path,
    }
    send_to_queue("ptm_queue", queue_msg)


def consume_pdf_processing_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)
        # Declare the queue
        channel.queue_declare(queue=QUEUE_NAME)
        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_book)

        print(f" [*] Waiting for messages on {QUEUE_NAME}. To exit, press CTRL+C")
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


if __name__ == "__main__":
    consume_pdf_processing_queue()
