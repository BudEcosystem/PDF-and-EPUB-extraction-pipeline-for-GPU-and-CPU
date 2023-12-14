# pylint: disable=all
# type: ignore
import json
from dotenv import load_dotenv
import os
import boto3
from rabbitmq_connection import get_rabbitmq_connection, get_channel
from utils import generate_image_str, get_mongo_collection


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

book_details = get_mongo_collection('book_details')
 

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
    publeynet_queue_message = {
        "job":'publeynet',
        "queue": queue_name,
        "image_path": image_path,
        "page_num": page_num,
        "bookname": bookname,
        "bookId": bookId,
        "total_pages":num_pages,
        "image_str": generate_image_str(image_path)
    }
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(publeynet_queue_message))
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
        "total_pages":num_pages,
        "image_str": generate_image_str(image_path)
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
        "total_pages":num_pages,
        "image_str": generate_image_str(image_path)
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
        "results": results,
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


def other_pages_queue(queue_name, page_data, total_other_pages, bookname, bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    other_pages_queue_data = {
        "queue": queue_name,
        "page_result":page_data,
        "total_other_pages":total_other_pages,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(other_pages_queue_data))
    print(f" [x] Sent {bookname} ({bookId}) to {queue_name}")
    connection.close()


def latex_ocr_queue(queue_name, page_result, total_latex_pages, bookname, bookId):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    latex_ocr_queue_data = {
        "queue": queue_name,
        "page_result":page_result,
        "total_latex_pages":total_latex_pages,
        "bookname": bookname,
        "bookId": bookId
    }

    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(latex_ocr_queue_data))
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
    # books= get_all_books_names(bucket_name, folder_name + '/')
    # books=books[127:]
#     books=["Guide to Competitive Programming - Antti Laaksonen.pdf","Guide to Computer Network Security - Joseph Migga Kizza.pdf","Guide to Discrete Mathematics - Gerard O'Regan.pdf","Guide to Scientific Computing in C++ - Joe Pitt-Francis- Jonathan Whiteley.pdf","Handbook of Consumer Finance Research - Jing Jian Xiao.pdf","Handbook of Disaster Research - Havidan Rodriguez- Enrico L Quarantelli- Russell Dynes.pdf","Handbook of Evolutionary Research in Archaeology - Anna Marie Prentiss.pdf","Handbook of LGBT Elders - Debra A Harley- Pamela B Teaster.pdf","Handbook of Marriage and the Family - Gary W Peterson- Kevin R Bush.pdf","Handbook of Quantitative Criminology - Alex R Piquero- David Weisburd.pdf","Handbook of the Life Course - Jeylan T Mortimer- Michael J Shanahan.pdf","International Business Management - Kamal Fatehi- Jeongho Choi.pdf","International Humanitarian Action - Hans-Joachim Heintze- Pierre Thielbörger.pdf","International Perspectives on Psychotherapy - Stefan G Hofmann.pdf","International Trade Theory and Policy - Giancarlo Gandolfo.pdf","Internet of Things From Hype to Reality - Ammar Rayes- Samer Salam.pdf","Introduction to Data Science - Laura Igual- Santi Seguí.pdf","Introduction to Deep Learning - Sandro Skansi.pdf","Introduction to Electronic Commerce and Social Commerce - Efraim Turban- Judy Whiteside- David King- Jon Outland.pdf","Introduction to Evolutionary Computing - AE Eiben- JE Smith.pdf","Introduction to Formal Philosophy - Sven Ove Hansson- Vincent F Hendricks.pdf","Introduction to General Relativity - Cosimo Bambi.pdf","Introduction to Law - Jaap Hage- Antonia Waltermann- Bram Akkermans.pdf",
# "Introduction to Mathematica® for Physicists - Andrey Grozin.pdf","Introduction to Parallel Computing - Roman Trobec- Boštjan Slivnik- Patricio Bulić- Borut Robič.pdf","Introduction to Partial Differential Equations - David Borthwick.pdf","Introduction to Programming with Fortran - Ian Chivers- Jane Sleightholme.pdf","Introduction to Smooth Manifolds - John Lee.pdf",
# "Introduction to Statistics and Data Analysis  - Christian Heumann- Michael Schomaker-  Shalabh.pdf","Introduction to Time Series and Forecasting - Peter J Brockwell- Richard A Davis.pdf"
# ,"Introductory Quantum Mechanics - Paul R Berman.pdf","Introductory Statistics with R - Peter Dalgaard.pdf","Introductory Time Series with R - Paul SP Cowpertwait- Andrew V Metcalfe.pdf",
# "Knowledge Management - Klaus North- Gita Kumta.pdf","Language Across the Curriculum & CLIL in English as an Additional Language (EAL) Contexts - Angel MY Lin.pdf"]
#     print(books)
#     for book in books:
#         book_data={
#             "bookId":uuid.uuid4().hex,
#             "book":book,
#             "status":"yet_extracted"
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
