""" This module contains the utility functions used in the pipeline. """
import os
import time
from functools import wraps
import cv2
import boto3
import pymongo
from PyPDF2 import PdfWriter, PdfReader
from uuid import uuid4
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']
bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']

mongo_connection_string = os.environ['DATABASE_URL']

pdf_batch_size = int(os.environ['PDF_BATCH_SIZE'])

# Create an S3 client
s3 = boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region)

def timeit(func):
    """
    Keeps track of the time taken by a function to execute.
    """
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

def crop_image(block, imagepath, id):
    """
    Function to crop the image based on the bounding box coordinates.
    """
    x1, y1, x2, y2 = block['x_1'], block['y_1'], block['x_2'], block['y_2']
    img = cv2.imread(imagepath)
    # Expand the bounding box by 5 pixels on every side
    x1-=5
    y1-=5
    x2+=5
    y2+=5

    # Ensure the coordinates are within the image boundaries
    x1=max(0,x1)
    y1=max(0,y1)
    x2=min(img.shape[1],x2)
    y2=min(img.shape[0],y2)

    #crop the expanded bounding box
    bbox = img[int(y1):int(y2), int(x1):int(x2)]
    cropped_image_path = f"cropeed{id}.png"
    cv2.imwrite(cropped_image_path,bbox)

    return cropped_image_path

def generate_unique_id():
    """ generate unique id """
    return uuid4().hex

@timeit
def download_book_from_aws(book_id, book_name):
    """
    Function used to download the book from AWS S3.
    """
    local_path = None
    try:
        print('AWS book download >> ', book_name)
        book_folder = os.path.join(folder_name, book_id)
        os.makedirs(book_folder, exist_ok=True)
        local_path = os.path.join(book_folder, book_name)
        file_key = f'{folder_name}/{book_name}'
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        pdf_data = response['Body'].read()
        with open(local_path, 'wb') as f:
            f.write(pdf_data)
    except Exception as e:
        print("An error occurred:", e)
    return local_path

@timeit
def split_pdf(local_path):
    """
    Function used to split the pdf into individual pages.
    """
    # book-set-2/123/abc.pdf
    print('Splitting pdf >> ', local_path)
    book_id = local_path.split('/')[1]
    book_split_folder = os.path.join(folder_name, book_id, 'splits')
    os.makedirs(book_split_folder, exist_ok=True)
    # book-set-2/123/splits
    print("split folder  >>> ", book_split_folder)

    with open(local_path, 'rb') as f:
        inputpdf = PdfReader(f)
        file_prefix = generate_unique_id()
        output_file_paths = []
        total_num_pages = len(inputpdf.pages)
        print("Total number of pages in the pdf: ", total_num_pages)
        if total_num_pages > pdf_batch_size:
            print("Splitting pdf into batches")
            for i in range(0, total_num_pages, pdf_batch_size):
                output = PdfWriter()
                for page in inputpdf.pages[i:i+pdf_batch_size]:
                    output.add_page(page)
                file_path = f"{book_split_folder}/{file_prefix}_{int(i/pdf_batch_size)}.pdf"
                with open(file_path, "wb") as output_stream:
                    output.write(output_stream)
                output_file_paths.append(file_path)
        else:
            output = PdfWriter()
            for page in inputpdf.pages:
                output.add_page(page)
            file_path = f"{book_split_folder}/{file_prefix}_0.pdf"
            with open(file_path, "wb") as output_stream:
                output.write(output_stream)
            output_file_paths.append(file_path)
        return output_file_paths

def get_mongo_client():
    """
    Function to get the mongo client.
    """
    mongo_client = pymongo.MongoClient(mongo_connection_string)
    return mongo_client

def get_mongo_collection(mongo_db, collection_name):
    """
    Function to get the mongo collection.
    """
    mongo_client = get_mongo_client()
    db = mongo_client[mongo_db]
    collection = db[collection_name]
    return collection

if __name__ == '__main__':
    BOOK_ID = '456'
    BOOK = 'output_2.pdf'
    BOOKS = [
        'Evidence-Based Critical Care - Robert C Hyzy.pdf',
        'Evidence-Based Interventions for Children with Challenging Behavior - Kathleen Hague Armstrong- Julia A Ogg- Ashley N Sundman-Wheat- Audra St John Walsh.pdf',
        'Evidence-Based Practice in Clinical Social Work - James W Drisko- Melissa D Grady.pdf',
        'Evolutionary Thinking in Medicine - Alexandra Alvergne- Crispin Jenkinson- Charlotte Faurie.pdf',
        'Exam Survival Guide: Physical Chemistry - Jochen Vogt.pdf'
        ]
    file_local_path = download_book_from_aws(BOOK_ID, BOOK)
    split_local_paths = split_pdf(file_local_path)
    print(split_local_paths)

