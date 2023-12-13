# pylint: disable=all
# type: ignore
import json
from dotenv import load_dotenv
import pymongo
import os
import boto3
import uuid
from rabbitmq_connection import get_rabbitmq_connection, get_channel


load_dotenv()

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']


# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.book_set_2

book_details = db.book_details
 


def get_all_books_names(bucket_name, folder_name):
  '''
    Get all books names from aws s3 bucket

    Args:
        bucket_name (str): The name of the AWS S3 bucket.
        folder_name (str): The name of the folder within the bucket.

    Returns:
        list: A list of dictionaries representing the contents (objects) in the specified folder.
    '''
  contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
  pdf_file_names = [obj['Key'] for obj in contents.get('Contents', [])]
  book_names = [file_name.split('/')[1] for file_name in pdf_file_names]
  return book_names


def send_pdf_to_queue(book):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    book['_id'] = str(book['_id'])
    pdf_message = {
        "queue": "pdf_processing_queue",
        'book': book
    }
    channel.queue_declare(queue='pdf_processing_queue')
    channel.basic_publish(exchange='', routing_key='pdf_processing_queue', body=json.dumps(pdf_message))
    print(f" [x] Sent {book['book']} ({book['bookId']}) to PDF processing queue")
    connection.close()
    

def publeynet_queue(queue_name,image_path,page_num,bookname,bookId,num_pages):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    table_bank_message = {
        "job":'publeynet',
        "queue": queue_name,
        "image_path": image_path,
        "page_num": page_num,
        "bookname": bookname,
        "bookId": bookId,
        "total_pages":num_pages
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(table_bank_message))
    print(f" [x] Sent {bookname} ({bookId}), Page {page_num} to {queue_name}")
    connection.close()

def table_bank_queue(queue_name,image_path,page_num,bookname,bookId,num_pages ):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    table_bank_message = {
        "job":'table_bank',
        "queue": queue_name,
        "image_path": image_path,
        "page_num": page_num,
        "bookname": bookname,
        "bookId": bookId,
        "total_pages":num_pages 
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(table_bank_message))
    print(f" [x] Sent {bookname} ({bookId}), Page {page_num} to table_bank_queue")
    connection.close()

def mfd_queue(queue_name,image_path,page_num,bookname,bookId,num_pages):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    mfd_message = {
        "job":'mfd',
        "queue": queue_name,
        "image_path": image_path,
        "page_num": page_num,
        "bookname": bookname,
        "bookId": bookId,
        "total_pages":num_pages
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(mfd_message))
    print(f" [x] Sent {bookname} ({bookId}), Page {page_num} to {queue_name}")
    connection.close()

def pdfigcap_queue(queue_name,pdf_path,bookname,bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    pdfigcapx_message = {
        "job":'pdfigcap',
        "queue": queue_name,
        "pdf_path": pdf_path,
        "bookname": bookname,
        "bookId": bookId
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(pdfigcapx_message))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()

def check_ptm_completion_queue(queue_name,bookname,bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    pdfigcapx_message = {
        "job":'check_ptm_completion',
        "queue": queue_name,
        "bookname": bookname,
        "bookId": bookId
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(pdfigcapx_message))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()

def book_completion_queue(queue_name,bookname,bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    book_completion_message = {
        "job":'book_completion_queue',
        "queue": queue_name,
        "bookname": bookname,
        "bookId": bookId
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(book_completion_message))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()

def nougat_queue(queue_name,image_path,total_nougat_pages,book_page_num, page_num,bookname,bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    nougat_message = {
        "job":'nougat_queue',
        "queue": queue_name,
        "image_path": image_path,
        "total_nougat_pages":total_nougat_pages,
        "book_page_num":book_page_num,
        "page_num":page_num,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(nougat_message))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def nougat_pdf_queue(queue_name,results,bookname,bookId):
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


def page_extraction_queue(queue_name,book_pages,bookname,bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    page_extraction_queue = {
        "queue": queue_name,
        "book_pages":book_pages,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(page_extraction_queue))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def other_pages_queue(queue_name,page_result, total_other_pages,page_num, bookname, bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    other_pages_queue = {
        "queue": queue_name,
        "page_result":page_result,
        "total_other_pages":total_other_pages,
        "page_num":page_num,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(other_pages_queue))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def latex_ocr_queue(queue_name, page_result, total_latex_pages, page_num, bookname, bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    latex_ocr_queue = {
        "queue": queue_name,
        "page_result":page_result,
        "total_latex_pages":total_latex_pages,
        "page_num":page_num,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(latex_ocr_queue))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def table_queue(queue_name, tableId, data, page_num, bookname, bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    table_queue = {
        "queue": queue_name,
        "tableId":tableId,
        "data":data,
        "page_num":page_num,
        "bookname": bookname,
        "bookId": bookId
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(table_queue))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def error_queue(queue_name, bookname, bookId,error):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    error_queue = {
        "queue": queue_name,
        "bookname":bookname,
        "bookId":bookId,
        "error": error
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(error_queue))
    print(f" [x] Sent {error} sent to {queue_name}")
    connection.close()

# def store_book_details():
#     # books= get_all_books_names(bucket_name, folder_name + '/')
#     # books=books[127:]
#     books=['Evidence-Based Critical Care - Robert C Hyzy.pdf','Evidence-Based Interventions for Children with Challenging Behavior - Kathleen Hague Armstrong- Julia A Ogg- Ashley N Sundman-Wheat- Audra St John Walsh.pdf','Evidence-Based Practice in Clinical Social Work - James W Drisko- Melissa D Grady.pdf','Evolutionary Thinking in Medicine - Alexandra Alvergne- Crispin Jenkinson- Charlotte Faurie.pdf','Exam Survival Guide: Physical Chemistry - Jochen Vogt.pdf']
#     print(books)
#     for book in books:
#         book_data={
#             "bookId":uuid.uuid4().hex,
#             "book":book,
#             "status":"not_extracted"
#         }
#         book_details.insert_one(book_data)

# # # store all books from aws to book_details collection before running
# store_book_details()

if __name__ == "__main__":
    try:

        books=book_details.find({})
        for book in books:
            if book['status']=='not_extracted':
                send_pdf_to_queue(book)

    except KeyboardInterrupt:
        pass
