import json
from rabbitmq_connection import get_rabbitmq_connection, get_channel

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

if __name__ == "__main__":
    try:
        bookname = "output_123.pdf"
        bookId = "123"
        results = [
            {"image_path": "/home/azureuser/prakash2/output_123/page_10.jpg", "page_num": "10"},
            {"image_path": "/home/azureuser/prakash2/output_123/page_11.jpg", "page_num": "11"},
            {"image_path": "/home/azureuser/prakash2/output_123/page_15.jpg", "page_num": "15"},
            {"image_path": "/home/azureuser/prakash2/output_123/page_16.jpg", "page_num": "16"}
        ]
        # nougat_pdf_queue_test("nougat_pdf_queue_test", results, bookname, bookId)
        # nougat_pdf_queue_test_bc_test('book_completion_queue')
    except KeyboardInterrupt:
        pass