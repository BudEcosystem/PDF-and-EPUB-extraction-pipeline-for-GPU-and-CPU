# import sys

# sys.path.append("pdf_extraction_pipeline")
import json
from utils import get_rabbitmq_connection, get_channel, get_mongo_collection

error_collection = get_mongo_collection("error_collection")


def nougat_pdf_queue_test(bookId, page_num, split_num, image_path, queue_name="nougat_queue"):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    nougat_pdf_queue_message = {
        "results": None,
        "is_figure_present": None,
        "bookId": bookId,
        "page_num": page_num,
        "splits": split_num,
        "image_path": image_path
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(
        exchange="", routing_key=queue_name, body=json.dumps(nougat_pdf_queue_message)
    )
    print(f" [x] Sent {bookId} to {queue_name}")
    connection.close()


def nougat_pdf_queue_test_bc_test(queue_name):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    nougat_pdf_queue_message = {
        "job": "book_completion_queue",
        "queue": "book_completion_queue",
        "bookname": "Foundations of Behavioral Health - Bruce Lubotsky Levin- Ardis Hanson.pdf",
        "bookId": "3e2a4c2bf26e40b5b900cb37df996c78",
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(
        exchange="", routing_key=queue_name, body=json.dumps(nougat_pdf_queue_message)
    )
    print(" [x] Sent bc_test")
    connection.close()


def re_queue_error(bookId):
    error_doc = error_collection.find_one({"bookId": bookId})
    if error_doc:
        errors = error_doc.get("errors", [])
        if errors:
            queue_name = "latex_ocr_queue"
            connection = get_rabbitmq_connection()
            channel = get_channel(connection)
            channel.queue_declare(queue=queue_name)
            for each in errors:
                queue_message = each["consumer_message"]
                channel.basic_publish(
                    exchange="", routing_key=queue_name, body=json.dumps(queue_message)
                )
                print(f" [x] Sent error to requeue")
            connection.close()
            error_collection.delete_one({"bookId": bookId})


def pdfigcapx_queue():
    split_paths = [
        "book-set-2/123/splits/123_0_1-15.pdf",
        "book-set-2/123/splits/123_1_16-30.pdf",
        "book-set-2/123/splits/123_2_31-45.pdf",
        "book-set-2/123/splits/123_3_46-60.pdf",
        "book-set-2/123/splits/123_4_61-75.pdf",
        "book-set-2/123/splits/123_5_76-90.pdf",
        "book-set-2/123/splits/123_6_91-105.pdf",
        "book-set-2/123/splits/123_7_106-120.pdf",
        "book-set-2/123/splits/123_8_121-135.pdf",
        "book-set-2/123/splits/123_9_136-150.pdf",
        "book-set-2/123/splits/123_10_151-165.pdf",
        "book-set-2/123/splits/123_11_166-180.pdf",
        "book-set-2/123/splits/123_12_181-195.pdf",
        "book-set-2/123/splits/123_13_196-210.pdf",
        "book-set-2/123/splits/123_14_211-225.pdf",
        "book-set-2/123/splits/123_15_226-240.pdf",
        "book-set-2/123/splits/123_16_241-255.pdf",
        "book-set-2/123/splits/123_17_256-270.pdf",
        "book-set-2/123/splits/123_18_271-278.pdf",
    ]
    for each in split_paths:
        bookId, _, fp, tp = get_page_num_from_split_path(each)
        send_to_queue(
            "pdfigcapx_queue",
            {"bookId": bookId, "split_path": each, "from_page": fp, "to_page": tp},
        )


def requeue_error(error_filter=None):
    for book in error_collection.find():
        bookId = book["bookId"]
        errors = book["errors"]
        if error_filter:
            errors = list(filter(lambda x: x["consumer"]==error_filter, errors))
        if errors:
            connection = get_rabbitmq_connection()
            channel = get_channel(connection)
            for error in errors:
                queue_name = error["consumer"]
                channel.queue_declare(queue=queue_name)
                queue_message = error["consumer_message"]
                channel.basic_publish(
                    exchange="", routing_key=queue_name, body=json.dumps(queue_message)
                )
                print(f" [x] Sent error to requeue to {queue_name}")
            error_collection.delete_one({"bookId": bookId})
            connection.close()

if __name__ == "__main__":
    try:
        # bookname = "output_123.pdf"
        # bookId = "d451b0398df04aeaa95c73c6982c82f5"
        # results = [
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_10.jpg", "page_num": "10"},
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_11.jpg", "page_num": "11"},
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_15.jpg", "page_num": "15"},
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_16.jpg", "page_num": "16"}
        # ]
        # nougat_pdf_queue_test("nougat_pdf_queue_test", results, bookname, bookId)
        # nougat_pdf_queue_test_bc_test('book_completion_queue')
        # re_queue_error(bookId)
        # nougat_pdf_queue_test(bookId, 398, None, "book-set-2/d451b0398df04aeaa95c73c6982c82f5/pages/page_398.jpg")
        requeue_error("bud_table_extraction_queue")
    except KeyboardInterrupt:
        pass
