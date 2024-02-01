# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import traceback
import shutil
import bson

from utils import timeit

load_dotenv()
import os
import json
from datetime import datetime
from pdf_pipeline.pdf_producer import error_queue
from utils import get_mongo_collection, get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

books = get_mongo_collection("libgen_data_2")
index_name = "index_bookId"
indexes_info = books.list_indexes()
index_exists = any(index_info["name"] == index_name for index_info in indexes_info)
if not index_exists:
    books.create_index("bookId", name=index_name, background=True)

book_details = get_mongo_collection("book_details")
nougat_pages = get_mongo_collection("nougat_pages")
other_pages = get_mongo_collection("other_pages")
latex_pages = get_mongo_collection("latex_pages")
book_images = get_mongo_collection("book_images")
text_pages = get_mongo_collection("text_pages")

QUEUE_NAME = "book_completion_queue"

MAX_BSON_SIZE = 16777216  # 16MB
BOOK_SPLIT_SIZE = 1000


def get_unique_pages(original_list):
    unique_values = set()
    result_list = []
    if original_list:
        for item in original_list:
            if (item['page_num'] not in unique_values):
                unique_values.add(item['page_num'])
                result_list.append(item)
    return result_list

@timeit
def book_complete(ch, method, properties, body):
    message = json.loads(body)
    bookId = message["bookId"]
    print(f"Received book completion request for {bookId}")

    try:
        book_already_completed = books.find_one({"bookId": bookId})
        if book_already_completed:
            print(f"book {bookId} already extracted")
            return
        book_det = book_details.find_one({"bookId": bookId})
        book_name = book_det["book"]
        book_path = book_det["book_path"]

        other_pages_result = []
        for doc in other_pages.find({"bookId": bookId}):
            doc_result = doc.get("pages", [])
            if doc_result:
                other_pages_result.extend(doc_result)
        latex_pages_result = []
        for doc in latex_pages.find({"bookId": bookId}):
            doc_result = doc.get("pages", [])
            if doc_result:
                latex_pages_result.extend(doc_result)
        text_pages_result = []
        for doc in text_pages.find({"bookId": bookId}):
            doc_result = doc.get("pages", [])
            if doc_result:
                text_pages_result.extend(doc_result)
        
        other_pages_result = get_unique_pages(other_pages_result)
        latex_pages_result = get_unique_pages(latex_pages_result)
        text_pages_result = get_unique_pages(text_pages_result)

        num_pages_done = len(other_pages_result) + len(latex_pages_result) + len(text_pages_result)
        print("num pages done > ", num_pages_done)
        num_nougat_pages_done = book_det.get("num_nougat_pages_done", 0)
        print("num_nougat_pages_done > ", num_nougat_pages_done)
        num_pages = book_det.get("num_pages")
        book_completed = False
        if (num_pages_done + num_nougat_pages_done) >= num_pages:
            book_completed = True
        if book_completed:
            nougat_pages_document = nougat_pages.find_one({"bookId": bookId})
            nougat_pages_result = []
            nougat_pages_result_dict = (
                nougat_pages_document.get("pages", []) if nougat_pages_document else []
            )
            if nougat_pages_result_dict:
                nougat_pages_result = [
                    result for _, result in nougat_pages_result_dict.items()
                ]
            all_pages = (
                other_pages_result
                + nougat_pages_result
                + latex_pages_result
                + text_pages_result
            )
            sorted_pages = sorted(all_pages, key=lambda x: int(x.get("page_num", 0)))
            new_document = {
                "bookId": bookId,
                "book": book_name,
                "pages": sorted_pages,
            }
            document_size = len(bson.BSON.encode(new_document))
            splits = 1
            if document_size >= MAX_BSON_SIZE:
                # make extracted book splits
                # Split the 'pages' array into batches
                page_batches = [sorted_pages[i:i + BOOK_SPLIT_SIZE] for i in range(0, len(sorted_pages), BOOK_SPLIT_SIZE)]
                splits = len(page_batches)
                for i, batch in enumerate(page_batches):
                    batch_doc = {
                        "bookId": bookId,
                        "book": book_name,
                        "split_order": i + 1,
                        "pages": batch
                    }
                    books.insert_one(batch_doc)
            else:
                books.insert_one(new_document)
            current_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            start_time = datetime.strptime(book_det["start_time"], "%d-%m-%Y %H:%M:%S")
            end_time = datetime.strptime(current_time, "%d-%m-%Y %H:%M:%S")
            total_time_taken = (end_time - start_time).total_seconds()
            book_details.update_one(
                {"bookId": bookId},
                {"$set": {"status": "post_process", "end_time": current_time, "time_taken": total_time_taken, "splits": splits}},
            )
            book_folder = os.path.dirname(book_path)
            if os.path.exists(book_folder):
                shutil.rmtree(book_folder)
            book_images.delete_many({"bookId": bookId})
        else:
            print(f"Book {bookId} not yet completed")
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno,
        }
        error_queue(book_name, bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_book_completion_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)

    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=book_complete)

    print(" [*] Waiting for messages on book_completion_queue. To exit, press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_book_completion_queue()
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()
