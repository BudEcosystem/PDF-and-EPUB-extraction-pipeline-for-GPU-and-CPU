import sys
sys.path.append("pdf_extraction_pipeline")
import json
from utils import get_rabbitmq_connection, get_channel, get_mongo_collection

error_collection = get_mongo_collection('error_collection')

def nougat_pdf_queue_test(queue_name, results, bookname, bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    nougat_pdf_queue_message = {
        "queue": queue_name,
        "results":results,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(nougat_pdf_queue_message))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def nougat_pdf_queue_test_bc_test(queue_name):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    nougat_pdf_queue_message = {"job": "book_completion_queue", "queue": "book_completion_queue", "bookname": "Foundations of Behavioral Health - Bruce Lubotsky Levin- Ardis Hanson.pdf", "bookId": "3e2a4c2bf26e40b5b900cb37df996c78"}

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(nougat_pdf_queue_message))
    print(f" [x] Sent bc_test")
    connection.close()


def re_queue_error(bookId):
    error_doc = error_collection.find_one({"bookId": bookId})
    if error_doc:
        errors = error_doc.get('errors', [])
        if errors:
            queue_name = "latex_ocr_queue"
            connection = get_rabbitmq_connection()
            channel = get_channel(connection)
            channel.queue_declare(queue=queue_name)
            for each in errors:
                queue_message = each['consumer_message']
                channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(queue_message))
                print(f" [x] Sent error to requeue")
            connection.close()
            error_collection.delete_one({"bookId": bookId})

if __name__ == "__main__":
    try:
        # bookname = "output_123.pdf"
        bookId = "fd58567c83c34a5cad2971178290ab05"
        # results = [
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_10.jpg", "page_num": "10"},
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_11.jpg", "page_num": "11"},
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_15.jpg", "page_num": "15"},
        #     {"image_path": "/home/azureuser/prakash2/output_123/page_16.jpg", "page_num": "16"}
        # ]
        # nougat_pdf_queue_test("nougat_pdf_queue_test", results, bookname, bookId)
        # nougat_pdf_queue_test_bc_test('book_completion_queue')
        re_queue_error(bookId)
    except KeyboardInterrupt:
        pass