# pylint: disable=all
# type: ignore
import traceback
import sys
import pytesseract
from PIL import Image

sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import os
import re
from utils import (
    timeit,
    get_mongo_collection,
    get_rabbitmq_connection,
    get_channel,
    generate_image_str,
    create_image_from_str,
)
import json
from pdf_producer import send_to_queue, error_queue

QUEUE_NAME = "text_pages_queue"

connection = get_rabbitmq_connection()
channel = get_channel(connection)

book_details = get_mongo_collection("book_details")
text_pages = get_mongo_collection("text_pages")


@timeit
def extract_text(ch, method, properties, body):
    message = json.loads(body)
    bookId = message["bookId"]
    page_num = message["page_num"]

    print(f"text pages received {page_num} : {bookId}")
    try:
        text_page = text_pages.find_one({"bookId": bookId, "pages.page_num": page_num})
        if text_page:
            print(f"text page {page_num} already extracted")
            send_to_queue("book_completion_queue", bookId)
            return
        page_obj = process_page(message, bookId)
        text_pages.find_one_and_update(
            {
                "bookId": bookId,
                "pages": {"$not": {"$elemMatch": {"page_num": page_obj["page_num"]}}}
            },
            {"$addToSet": {"pages": page_obj}},
            upsert=True
        )
        # document = text_pages.find_one({"bookId": bookId})
        # if document:
        #     text_pages.update_one(
        #         {"_id": document["_id"]}, {"$push": {"pages": page_obj}}
        #     )
        # else:
        #     new_book_document = {"bookId": bookId, "pages": [page_obj]}
        #     text_pages.insert_one(new_book_document)
        # book_details.find_one_and_update(
        #     {"bookId": bookId}, {"$inc": {"num_pages_done": 1}}
        # )
        send_to_queue("book_completion_queue", bookId)
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "page_num": page_num,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno,
        }
        error_queue("", bookId, error)
    finally:
        print("ack sent")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def process_page(page, bookId):
    page_obj = {}
    page_num = page["page_num"]
    # is_figure_present = page["is_figure_present"]
    image_str = generate_image_str(bookId, page["image_path"])
    new_image_path = create_image_from_str(image_str)
    image_data = Image.open(new_image_path)
    page_content = pytesseract.image_to_string(image_data)
    page_content = re.sub(r"\s+", " ", page_content).strip()
    page_obj = {
        "page_num": page_num,
        "text": page_content,
        "tables": [],
        "figures": [],
        "equations": [],
    }
    os.remove(new_image_path)
    return page_obj


def consume_text_pages_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)
        # Declare the queue
        channel.queue_declare(queue=QUEUE_NAME)

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=extract_text)

        print(f" [*] Waiting for messages on {QUEUE_NAME} To exit, press CTRL+C")
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


if __name__ == "__main__":
    consume_text_pages_queue()
