import os
import json
import regex as re
import boto3
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']
aws_bucket_name = os.environ['AWS_BUCKET_NAME']
aws_book_folder = os.environ['BOOK_FOLDER_NAME']

# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

# Connect to MongoDB
MONGO_URL = 'mongodb+srv://sonaligupta:Q8CXEyuEsQK2N611@cluster0.xhtq2pr.mongodb.net/'
client = MongoClient(MONGO_URL)
db = client['books']
collection = db['publisher_details']
error_collection = db['error_details']

def save_books_to_mongodb(folder_path: str):
    """Save book id, title and publisher to MongoDB collection

    Args:
        folder_path (str): Path to folder containing JSON files
    """
    # Iterate over all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)

            # Read the JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            data_page_json = json_data['data']['products']

            book_details = []
            for book in data_page_json:
                # Extract book_id and title
                book_id = book.get('product_id')
                title = book.get('title')
                custom_attr = book.get('custom_attributes')
                publishers = custom_attr.get('publishers') # is list

                book_details.append({
                    'book_id': book_id,
                    'title': title,
                    'publishers': publishers})

            # Insert into MongoDB collection
            collection.insert_many(book_details)

            print(f'Inserted data from {filename} into MongoDB collection')

def get_books_by_publisher(publisher: str):
    """Get books by publisher

    Args:
        publisher (str): Publisher name
    """
    # Find documents with publisher starting with "O'Reilly Media"
    result = collection.find({
        "publishers": {"$regex": f"^{publisher}"}
    })
    books = []
    for document in result:
        print(document['book_id'])
        print(document['title'])
        books.append(document)
    print(len(books))

def get_s3_url(next_token: str = ""):
    """Get S3 URL for book

    Returns:
        str: S3 URL
    """
    # list objects in bucket with prefix
    pattern = r'\((\w+)\)$'
    if next_token:
        response = s3.list_objects_v2(
            Bucket=aws_bucket_name,
            Prefix=aws_book_folder,
            ContinuationToken=next_token
        )
    else:
        response = s3.list_objects_v2(
            Bucket=aws_bucket_name,
            Prefix=aws_book_folder
        )
    next_token = response.get('NextContinuationToken')
    print(next_token)
    for obj in response['Contents']:
        key_split = obj['Key'].split('/')
        if len(key_split) > 4:
            continue
        if len(key_split) == 4 and key_split[3].endswith('.epub'):
            book_id = key_split[2]
            if '(' in book_id:
                matches = re.findall(pattern, book_id)
                if matches:
                    book_id = matches[0]
                else:
                    error_collection.insert_one({
                        "s3_key": obj['Key']
                    })
                    continue
            print(book_id)
            book = collection.find_one({"book_id": book_id})
            if book:
                collection.update_one(
                    {"book_id": book_id},
                    {"$set": {
                        "s3_key": obj['Key']
                        }
                    }
                )
            else:
                error_collection.insert_one({
                    "book_id": book_id,
                    "s3_key": obj['Key']
                })
    return next_token

def download_s3_book(s3_key: str, local_path: str):
    """Download book from S3

    Args:
        s3_url (str): S3 URL
        local_path (str): Local path to save book
    """
    # Download book from S3
    response = s3.get_object(Bucket=aws_bucket_name, Key=s3_key)
    epub_data = response['Body'].read()
    with open(local_path, 'wb') as file:
        file.write(epub_data)

def handle_errors():
    """Get S3 URL for book

    Args:
        book_id (str): Book ID

    Returns:
        str: S3 URL
    """
    # Find document with book_id
    results = error_collection.find({})
    for result in results:
        book_id = result.get('book_id')
        s3_key = result.get('s3_key')
        if s3_key:
            if not book_id:
                key_split = s3_key.split('/')
                matches = re.findall(r'\((\w+)\)$', key_split[2])
                print(matches)
                if matches:
                    book_id = matches[0]
                    collection.update_one(
                        {"book_id": book_id},
                        {"$set": {
                            "s3_key": s3_key
                            }
                        }
                    )
                    error_collection.delete_one({"s3_key": s3_key})

def list_objects(bucket_name, prefix=''):
    """
    List all objects in an S3 bucket.
    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str, optional): Prefix for searching. Defaults to ''.
    """
    s3 = boto3.client('s3')

    # Specify the bucket name and optional prefix
    params = {'Bucket': bucket_name}
    if prefix:
        params['Prefix'] = prefix
    pattern = r'\(([0-9a-zA-Z_]+)\)$'
    while True:
        # Make the initial or subsequent call to list_objects_v2
        response = s3.list_objects_v2(**params)

        # Process the results
        for obj in response.get('Contents', []):
            key_split = obj['Key'].split('/')
            if len(key_split) == 4 and key_split[3].endswith('.epub'):
                book_id = key_split[2]
                if '(' in book_id:
                    matches = re.findall(pattern, book_id)
                    if matches:
                        book_id = matches[0]
                    else:
                        error_collection.insert_one({
                            "s3_key": obj['Key']
                        })
                        continue
                else:
                    book = collection.find_one({"book_id": book_id, "s3_key": {"$exists": False}})
                    if book:
                        collection.update_one(
                            {"book_id": book_id},
                            {"$set": {
                                "s3_key": obj['Key']
                                }
                            }
                        )
                    else:
                        error_collection.insert_one({
                            "book_id": book_id,
                            "s3_key": obj['Key']
                        })

                    

        # Check if there are more results (pagination)
        if response.get('IsTruncated', False):
            # Set ContinuationToken for the next call
            params['ContinuationToken'] = response['NextContinuationToken']
        else:
            break 


def get_id_s3_key_mismatch():
    """Get books with mismatch in book_id and title
    """
    results = collection.find({})
    for result in results:
        book_id = result.get('book_id')
        title = result.get('title')
        s3_key = result.get('s3_key')
        if book_id and s3_key:
            if book_id not in s3_key:
                print(book_id)
                print(s3_key)
                error_collection.insert_one({
                    "book_id": book_id,
                    "title": title,
                    "s3_key": s3_key
                })

if __name__ == '__main__':
    # Specify the folder path
    # folder_path = '../bud-datalake/ebook_jsons'
    # save_books_to_mongodb(folder_path)
    # publisher_name = "O'Reilly Media"
    # get_books_by_publisher(publisher_name)
    # list_objects(aws_bucket_name, aws_book_folder)
    pass
