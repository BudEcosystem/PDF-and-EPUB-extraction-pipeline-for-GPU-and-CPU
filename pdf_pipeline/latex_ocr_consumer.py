# pylint: disable=all
# type: ignore
import traceback
import sys

sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import os
import re
from utils import (
    timeit,
    create_image_from_str,
    generate_image_str,
    get_mongo_collection,
    get_rabbitmq_connection,
    get_channel,
)
import json
from pdf_producer import send_to_queue, error_queue
from element_extraction_utils import (
    process_table,
    # process_figure,
    process_publaynet_figure,
    process_text,
    process_title,
    process_list,
    process_equation,
)

QUEUE_NAME = "latex_ocr_queue"

connection = get_rabbitmq_connection()
channel = get_channel(connection)

book_details = get_mongo_collection("book_details")
latex_pages = get_mongo_collection("latex_pages")
index_name = "index_bookId_pageNo"
indexes_info = latex_pages.list_indexes()
index_exists = any(index_info["name"] == index_name for index_info in indexes_info)
if not index_exists:
    latex_pages.create_index(["bookId", "pages.page_num"], name=index_name, background=True)


@timeit
def extract_latex_pages(ch, method, properties, body):
    message = json.loads(body)
    bookId = message["bookId"]
    page_num = message["page_num"]

    print(f"latex received {page_num} : {bookId}")
    try:
        latex_page = latex_pages.find_one(
            {"bookId": bookId, "pages.page_num": page_num}
        )
        if latex_page:
            print(f"latex page {page_num} already extracted")
            send_to_queue("book_completion_queue", bookId)
            return
        page_obj = process_page(message, bookId)
        latex_pages.find_one_and_update(
            {
                "bookId": bookId,
                "pages": {"$not": {"$elemMatch": {"page_num": page_obj["page_num"]}}},
            },
            {"$addToSet": {"pages": page_obj}},
            upsert=True,
        )
        # document = latex_pages.find_one({"bookId": bookId})
        # if document:
        #     latex_pages.update_one(
        #         {"_id": document["_id"]}, {"$push": {"pages": page_obj}}
        #     )
        # else:
        #     new_book_document = {"bookId": bookId, "pages": [page_obj]}
        #     latex_pages.insert_one(new_book_document)
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


@timeit
def process_page(page, bookId):
    page_obj = {}
    results = page["results"]
    page_num = page["page_num"]
    # is_figure_present = page["is_figure_present"]
    # image_str = generate_image_str(bookId, page["image_path"])
    # new_image_path = create_image_from_str(image_str)
    new_image_path = page["image_path"]
    page_content, figures, equations = sort_text_blocks_and_extract_data(
        results, new_image_path, bookId, page_num
    )
    page_obj = {
        "page_num": page_num,
        "text": page_content,
        "tables": [],
        "figures": figures,
        "equations": equations,
    }
    # os.remove(new_image_path)
    return page_obj


@timeit
def sort_text_blocks_and_extract_data(blocks, image_path, bookId, page_num):
    page_content = ""
    figures = []
    equations = []
    sorted_blocks = sorted(blocks, key=lambda block: (block["y_1"] + block["y_2"]) / 2)
    for i, block in enumerate(sorted_blocks):
        if block["type"] == "Table":
            output = process_table(block, image_path, bookId, page_num)
        elif block["type"] == "Figure":
            output, figure = process_publaynet_figure(block, image_path)
            figures.append(figure)
        elif block["type"] == "Text":
            output = process_text(block, image_path)
        elif block["type"] == "Title":
            output = process_title(block, image_path)
        elif block["type"] == "List":
            output = process_list(block, image_path)
        elif block["type"] == "Equation":
            output, equation = process_equation(block, image_path)
            equations.append(equation)
        page_content += output
    page_content = re.sub(r"\s+", " ", page_content).strip()
    return page_content, figures, equations


def consume_latex_ocr_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)

    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=extract_latex_pages)

    print(f" [*] Waiting for messages on {QUEUE_NAME} To exit, press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_latex_ocr_queue()
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()
