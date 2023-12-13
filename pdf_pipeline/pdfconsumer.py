# pylint: disable=all
# type: ignore
import json
import sys
import os
import traceback
from PyPDF2 import PdfReader
import fitz
import boto3
from datetime import datetime

from dotenv import load_dotenv
sys.path.append("pdf_extraction_pipeline")
from utils import timeit
import pymongo
from pdf_producer import table_bank_queue, pdfigcap_queue, mfd_queue, publeynet_queue, error_queue, check_ptm_completion_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']

# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.book_set_2

error_collection = db.error_collection
book_details=db.book_details
figure_caption = db.figure_caption
table_bank_done=db.table_bank_done
publaynet_done=db.publaynet_done
mfd_done=db.mfd_done
bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']



@timeit
def download_book_from_aws(bookname, bookId):
  try:
    print('bookname')
    os.makedirs(folder_name, exist_ok=True)
    local_path = os.path.join(folder_name, f'{bookname}')
    file_key = f'{folder_name}/{bookname}'
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    pdf_data = response['Body'].read()
    with open(local_path, 'wb') as f:
      f.write(pdf_data)
    return local_path   
  except Exception as e:
    print("An error occurred:", e)
    error = {"consumer":"pdf_consumer","error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
    error_queue('error_queue',bookname, bookId, error)

@timeit
def process_book(ch, method, properties, body): 
    try:
        message = json.loads(body)
        book = message["book"]
        bookname = book['book']
        bookId = book['bookId']
        
        current_time = datetime.now().strftime("%H:%M:%S")
        print(current_time)
        book_details.update_one(
            {'bookId': bookId},
            {'$set': {'status': 'processing','start_time': current_time}}
        )
        book_folder = bookname.replace('.pdf', '')
        book_path = download_book_from_aws(bookname, bookId)
        if not book_path:
            raise Exception("Book not found") 
        os.makedirs(book_folder, exist_ok=True)
        book = PdfReader(book_path)  
        print(bookname)
       
        num_pages = len(book.pages)
        print(f"{bookname} has total {num_pages} page")  
        if num_pages>15:
            pdfigcap_queue('pdfigcap_queue',book_path,bookname, bookId)
        else:
            figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": [], "status":"failed"})
        
        publaynet=False
        mfd=False
        tableBank=False
        #check if publaynet, tablebank and mfd extraction done for current book
        publeynet_done_document = publaynet_done.find_one({"bookId": bookId})
        table_done_document = table_bank_done.find_one({"bookId": bookId})
        mfd_done_document = mfd_done.find_one({"bookId": bookId})   
        if publeynet_done_document:
            publaynet=True
            print("Publaynet extraction already exist for this book")
        if table_done_document:
            tableBank=True
            print("tablebank extraction already exist for this book")
        if mfd_done_document:
            mfd=True
            print("mfd extraction already exist for this book")
        for page_num in range(0,num_pages):
            process_page(page_num, book_path, book_folder, bookname, bookId,num_pages,publaynet,mfd,tableBank)
    except Exception as e:
        error = {"consumer":"pdf_consumer","consumer_message":message,"error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
        print(error) 
        error_queue('error_queue', bookname, bookId, error)   
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

  
@timeit
def process_page(page_num, book_path, book_folder, bookname, bookId,num_pages, publaynet,mfd, tableBank):
    pdf_book = fitz.open(book_path)
    page_image = pdf_book[page_num]
    book_image = page_image.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
    image_path = os.path.join(book_folder, f'page_{page_num + 1}.jpg')
    book_image.save(image_path)
    absolute_image_path = os.path.abspath(image_path)
    if not publaynet:
        publeynet_queue('publeynet_queue',absolute_image_path, page_num, bookname, bookId,num_pages)
    if not tableBank:
        table_bank_queue('table_bank_queue',absolute_image_path, page_num, bookname, bookId, num_pages )
    if not mfd: 
        mfd_queue('mfd_queue',absolute_image_path, page_num, bookname, bookId,num_pages )


def consume_pdf_processing_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)
        # Declare the queue
        channel.queue_declare(queue='pdf_processing_queue')
        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='pdf_processing_queue', on_message_callback=process_book)

        print(' [*] Waiting for messages on pdf_processing_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    try:
        consume_pdf_processing_queue()
    except KeyboardInterrupt:
        pass
