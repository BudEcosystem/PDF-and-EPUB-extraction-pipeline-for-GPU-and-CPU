# pylint: disable=all
# type: ignore
import os
import traceback
import sys

sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import json
from pdf_producer import error_queue, send_to_queue
from utils import get_mongo_collection, get_rabbitmq_connection, get_channel
from dotenv import load_dotenv

load_dotenv()

QUEUE_NAME = "check_ptm_completion_queue"

nougat_batch_size = int(os.environ["NOUGAT_BATCH_SIZE"])

connection = get_rabbitmq_connection()
channel = get_channel(connection)

book_details = get_mongo_collection("book_details")
figure_caption = get_mongo_collection("figure_caption")
publaynet_pages = get_mongo_collection("publaynet_pages")
table_bank_pages = get_mongo_collection("table_bank_pages")
mfd_pages = get_mongo_collection("mfd_pages")


# def get_fig_data(bookId, book_path):
#     fig_done = False
#     fig = figure_caption.find_one({"bookId": bookId, "split_path": book_path})
#     if fig:
#         fig_done = True
#     return fig_done, fig


def get_ptm_data(layout_name, bookId, page_num):
    layout_done = False
    find_data = {"bookId": bookId, "pages.page_num": page_num}
    data = None
    if layout_name == "publaynet":
        data = publaynet_pages.find_one(find_data)
    elif layout_name == "table_bank":
        data = table_bank_pages.find_one(find_data)
    elif layout_name == "mfd":
        data = mfd_pages.find_one(find_data)
    if data:
        layout_done = True
        data = list(filter(lambda x: x["page_num"] == page_num, data["pages"]))
        # no check because layout block for page_num should always be present
        data = data[0]
    return layout_done, data


def check_ptm_status(ch, method, properties, body):
    print("hello pmt called")
    message = json.loads(body)
    bookId = message["bookId"]
    book_path = message["split_path"]
    page_num = message["page_num"]  # can be None if request comes from pdfigcapx
    try:
        page_data = check_ptm(page_num, bookId)
        if page_data:
            page_data["split_path"] = book_path
            process_page(page_data)
        else:
            print(
                f"mfd, tablebank and publaynet extraction for {page_num} - {bookId} not yet completed"
            )
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno,
        }
        error_queue(book_path, bookId, error)
    finally:
        print("message ack sent")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def check_ptm(page_no, bookId):
    process_page_data = None
    publaynet_done, publaynet_data = get_ptm_data("publaynet", bookId, page_no)
    table_bank_done, table_bank_data = get_ptm_data("table_bank", bookId, page_no)
    mfd_done, mfd_data = get_ptm_data("mfd", bookId, page_no)
    if publaynet_done and table_bank_done and mfd_done:
        publaynet_page_results = publaynet_data["result"]
        table_bank_page_results = table_bank_data["result"]
        mfd_page_results = mfd_data["result"]
        page_results = (
            publaynet_page_results + table_bank_page_results + mfd_page_results
        )
        image_path = publaynet_data["image_path"]
        if not page_results:
            page_results = [{"image_path": image_path}]
        else:
            for each in page_results:
                each["image_path"] = image_path
        process_page_data = {
            "page_num": page_no,
            "results": page_results,
            "image_path": image_path,
            "bookId": bookId,
        }
    return process_page_data


def process_page(process_page_data, fig_result):
    page_num = process_page_data["page_num"]
    results = process_page_data["results"]
    image_path = process_page_data["image_path"]
    bookId = process_page_data["bookId"]

    # is_figure_present = False
    # if fig_result:
    #     is_figure_present = True
    #     # if pdfigcapx find figure then remove figures identified by publaynet
    #     # and add pdfigcapx results
    #     results = [
    #         block for block in results if "type" in block and block["type"] != "Figure"
    #     ]
    #     for each in fig_result:
    #         each["image_path"] = image_path
    #     # results.extend(fig_result['result'])
    #     results.extend(fig_result)
    book_data = book_details.find_one({"bookId": bookId})
    num_nougat_pages = book_data.get("num_nougat_pages", [])
    num_latex_pages = book_data.get("num_latex_pages", [])
    num_other_pages = book_data.get("num_other_pages", [])
    if (
        not results
        or not any("x_1" in block and "y_1" in block for block in results)
        or not any(block["type"] in ["Table", "Figure"] for block in results)
    ):
        nougat_queue_msg = {
            "image_path": image_path,
            "page_num": page_num,
            "bookId": bookId,
        }
        if page_num not in num_nougat_pages:
            book_details.find_one_and_update(
                {"bookId": bookId},
                {
                    "$addToSet": {
                        "nougat_pages": nougat_queue_msg,
                        "num_nougat_pages": page_num,
                    }
                },
            )
            split_id = calculate_split_id(nougat_queue_msg)
            nougat_queue_msg["split_id"] = split_id
            send_to_queue("nougat_queue", nougat_queue_msg)
    elif any(block["type"] == "Equation" for block in results):
        latex_ocr_queue_msg = {
            "results": results,
            "image_path": image_path,
            "bookId": bookId,
            "page_num": page_num,
            # "is_figure_present": is_figure_present,
        }
        if page_num not in num_latex_pages:
            book_details.find_one_and_update(
                {"bookId": bookId}, {"$addToSet": {"num_latex_pages": page_num}}
            )
            send_to_queue("latex_ocr_queue", latex_ocr_queue_msg)
    else:
        other_pages_queue_msg = {
            "results": results,
            "image_path": image_path,
            "bookId": bookId,
            "page_num": page_num,
            # "is_figure_present": is_figure_present,
        }
        if page_num not in num_other_pages:
            book_details.find_one_and_update(
                {"bookId": bookId}, {"$addToSet": {"num_other_pages": page_num}}
            )
            send_to_queue("other_pages_queue", other_pages_queue_msg)
    book_data = book_details.find_one({"bookId": bookId})
    total_pages_in_book = book_data["num_pages"]
    num_nougat_pages = len(book_data.get("num_nougat_pages", []))
    num_latex_pages = len(book_data.get("num_latex_pages", []))
    num_other_pages = len(book_data.get("num_other_pages", []))
    if (
        num_nougat_pages > 0
        and num_nougat_pages + num_latex_pages + num_other_pages == total_pages_in_book
    ):
        send_to_queue("nougat_queue", {"bookId": bookId})


def calculate_split_id(nougat_page: dict) -> str:
    bookId = nougat_page["bookId"]
    page_num = nougat_page["page_num"]
    book = book_details.find_one({"bookId": bookId})
    nougat_splits = book.get("nougat_splits", {})  # {0: [], 1: [], ...}
    split_id = None
    if not nougat_splits:
        split_id = 0
        nougat_splits[str(split_id)] = [nougat_page]
    else:
        # TODO: optimize with indexing or other methods
        page_exists = False
        for s_id, pages in nougat_splits.items():
            for page in pages:
                if page["page_num"] == page_num:
                    split_id = s_id
                    page_exists = True
                    break
        if page_exists:
            return split_id
        sorted_split_ids = sorted(map(int, nougat_splits.keys()))
        largest_split_id = sorted_split_ids[-1]
        pages = nougat_splits[str(largest_split_id)]
        if len(pages) < nougat_batch_size:
            split_id = largest_split_id
            nougat_splits[str(largest_split_id)].append(nougat_page)
        else:
            next_split_id = largest_split_id + 1
            split_id = next_split_id
            nougat_splits[str(next_split_id)] = [nougat_page]
    book_details.update_one(
        {"bookId": bookId}, {"$set": {"nougat_splits": nougat_splits}}
    )
    return str(split_id)


def consume_ptm_completion_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)

    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=check_ptm_status)

    print(f" [*] Waiting for messages on {QUEUE_NAME}. To exit, press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_ptm_completion_queue()
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()
