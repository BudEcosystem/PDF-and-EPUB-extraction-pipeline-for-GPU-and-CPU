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
    get_channel
)
import json
from pdf_producer import send_to_queue, error_queue
from element_extraction_utils import (
    process_table,
    process_figure,
    process_publaynet_figure,
    process_text,
    process_title,
    process_list
)



connection = get_rabbitmq_connection()
channel = get_channel(connection)

book_details = get_mongo_collection('book_details')
other_pages = get_mongo_collection('other_pages')


@timeit
def extract_other_pages(ch, method, properties, body):
    message = json.loads(body)
    bookId = message["bookId"]
    page_num = message["page_num"]

    print(f"other pages received {page_num} : {bookId}")
    try:
        other_page = other_pages.find_one({"bookId": bookId, "pages.page_num": page_num})
        if other_page:
            print(f"other page {page_num} already extracted")
            send_to_queue("book_completion_queue", bookId)
            return
        page_obj = process_page(message, bookId)
        document = other_pages.find_one({'bookId': bookId})
        if document:
            other_pages.update_one({"_id":document["_id"]}, {"$push": {"pages": page_obj}})
        else:
            new_book_document = {
                "bookId": bookId,
                "pages": [page_obj]
            }
            other_pages.insert_one(new_book_document)
        book_details.find_one_and_update(
            {"bookId": bookId},
            {
                "$inc": {"num_pages_done": 1}
            }
        )
        send_to_queue('book_completion_queue', bookId)
    except Exception as e:
        error = {
            "consumer": "other_pages_consumer",
            "consumer_message": message,
            "page_num": page_num,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno
        } 
        error_queue('', bookId, error)
    finally:
        print("ack sent")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def process_page(page, bookId):
    page_obj = {}
    results = page["results"]
    page_num = page["page_num"]
    is_figure_present = page["is_figure_present"]
    image_str = generate_image_str(bookId, page["image_str"])
    new_image_path = create_image_from_str(image_str)
    
    page_content, figures = sort_text_blocks_and_extract_data(
        results, new_image_path, is_figure_present, bookId, page_num
    )
    page_obj = {
        "page_num": page_num,
        "text": page_content,
        "tables": [],
        "figures": figures,
        "equations": []
    }
    os.remove(new_image_path)
    return page_obj


def sort_text_blocks_and_extract_data(blocks, image_path, is_figure_present, bookId, page_num):
    page_content = ""
    figures = []
    prev_block = None
    next_block = None
    sorted_blocks = sorted(blocks, key = lambda block: (block['y_1'] + block['y_2']) / 2)
    for i, block in enumerate(sorted_blocks): 
        if i > 0:
            prev_block = sorted_blocks[i - 1]
        if i < len(sorted_blocks) - 1:
            next_block = sorted_blocks[i + 1]  
        if block['type'] == "Table":
            output = process_table(block, image_path, bookId, page_num)
        elif block['type'] == "Figure":
            if is_figure_present:
                output, figure = process_figure(block, image_path)
            else:
                output, figure = process_publaynet_figure(block, image_path, prev_block, next_block)  
            figures.append(figure)
        elif block['type'] == "Text":
            output = process_text(block, image_path)
        elif block['type'] == "Title":
            output = process_title(block, image_path)
        elif block['type'] == "List":
            output = process_list(block, image_path)
        page_content += output
    page_content = re.sub(r'\s+', ' ', output).strip()
    return page_content, figures


def consume_other_pages_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)
        # Declare the queue
        channel.queue_declare(queue='other_pages_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='other_pages_queue', on_message_callback=extract_other_pages)

        print(' [*] Waiting for messages on other_pages_queue To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


if __name__ == "__main__":
    consume_other_pages_queue()
