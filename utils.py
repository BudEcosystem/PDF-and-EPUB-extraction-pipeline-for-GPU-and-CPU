""" This module contains the utility functions used in the pipeline. """
import os
import time
from functools import wraps
import pika
import cv2
import boto3
import pymongo
from PyPDF2 import PdfWriter, PdfReader
from uuid import uuid4
from dotenv import load_dotenv
# sonali: depenedncy for utils currently commented out
import base64
import numpy as np

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']
bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']

mongo_connection_string = os.environ['DATABASE_URL']
mongo_db_name = os.environ['MONGO_DB']

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

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    
    # Specify the credentials in the connection parameters
    connection_params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        # Replace with the appropriate port
        heartbeat=0,
        credentials=credentials,
        connection_attempts=3
    )
    return pika.BlockingConnection(connection_params)

def get_channel(connection):
    return connection.channel()


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
    cropped_image_path = os.path.abspath(f"cropeed{id}.png")
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
        local_path = os.path.join(book_folder, f"{book_id}.pdf")
        # book-set-2/123/123.pdf
        file_key = f'{folder_name}/{book_name}'
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        pdf_data = response['Body'].read()
        with open(local_path, 'wb') as f:
            f.write(pdf_data)
    except Exception as e:
        print("An error occurred:", e)
    return local_path

def get_page_num_from_split_path(split_path):
    """
    Function is used to get book_id, from_page and to_page from book path
    """
    # book-set-2/123/splits/123_0_1-30.pdf
    parts = split_path.split('/')
    if len(parts) > 1:
        # it means input if filename and not complete path
        # 123_0_1-30.pdf
        filename = parts[-1]
    else:
        filename = split_path
    filename_split = filename.replace(".pdf", "").split("_")
    book_id = filename_split[0]
    split_id = filename_split[1]
    page_nums = filename_split[-1].split("-")
    from_page = int(page_nums[0])
    to_page = int(page_nums[-1])
    return book_id, split_id, from_page, to_page

@timeit
def split_pdf(book_id, local_path):
    """
    Function used to split the pdf into individual pages.
    """
    # book-set-2/123/123.pdf
    print('Splitting pdf >> ', local_path)
    book_split_folder = os.path.join(folder_name, book_id, 'splits')
    os.makedirs(book_split_folder, exist_ok=True)
    # book-set-2/123/splits
    print("split folder  >>> ", book_split_folder)

    with open(local_path, 'rb') as f:
        inputpdf = PdfReader(f)
        output_file_paths = []
        total_num_pages = len(inputpdf.pages)
        print("Total number of pages in the pdf: ", total_num_pages)
        if total_num_pages > pdf_batch_size:
            print("Splitting pdf into batches")
            for i in range(0, total_num_pages, pdf_batch_size):
                output = PdfWriter()
                from_page = i+1
                to_page = from_page
                for page in inputpdf.pages[i:i+pdf_batch_size]:
                    output.add_page(page)
                    to_page += 1
                file_path = f"{book_split_folder}/{book_id}_{int(i/pdf_batch_size)}_{from_page}-{to_page}.pdf"
                with open(file_path, "wb") as output_stream:
                    output.write(output_stream)
                output_file_paths.append(file_path)
        else:
            output = PdfWriter()
            from_page = 1
            to_page = from_page
            for page in inputpdf.pages:
                output.add_page(page)
                to_page += 1
            file_path = f"{book_split_folder}/{book_id}_0_{from_page}-{to_page}.pdf"
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

def get_mongo_collection(collection_name):
    """
    Function to get the mongo collection.
    """
    mongo_client = get_mongo_client()
    db = mongo_client[mongo_db_name]
    collection = db[collection_name]
    return collection

def read_image_from_str(image_str):
    image_bytes = base64.b64decode(image_str)
    image_np_array = np.frombuffer(image_bytes, np.uint8)
    image = cv2.imdecode(image_np_array, cv2.IMREAD_COLOR)
    return image

def generate_image_str(book_id, image_path):
    image_str = None
    book_images_collection = get_mongo_collection(f'{book_id}_images')
    image_data = book_images_collection.find_one({
        "bookId": book_id,
        "image_path": image_path
    })
    if image_data:
        image_str = image_data["image_str"]
    else:
        with open(image_path, 'rb') as img:
            img_data = img.read()
        image_str = base64.b64encode(img_data).decode('utf-8')
        book_images_collection.insert_one({
            "bookId": book_id,
            "image_path": image_path,
            "image_str": image_str
        })
    return image_str

def create_image_from_str(image_str):
    image_data = base64.b64decode(image_str)
    image_path=f"{generate_unique_id()}.jpg"
    with open(image_path, 'wb') as img:
        img.write(image_data)
    return image_path

def find_split_path(split_paths, page_num):
    for split_path in split_paths:
        _, _, fp, tp = get_page_num_from_split_path(split_path)
        if page_num >= fp and page_num <= tp:
            return split_path

def upload_to_aws_s3(image_path, image_id): 
    image_folder_name = os.environ['AWS_PDF_IMAGE_UPLOAD_FOLDER']
    s3_key = f"{image_folder_name}/{image_id}.png"
    # Upload the image to the specified S3 bucket
    s3.upload_file(image_path, bucket_name, s3_key)
    # Get the URL of the uploaded image
    image_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
    return image_url


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
    # connection = get_rabbitmq_connection()
    # print("RabbitMQ connection established")

