import os
import json
from pymongo import MongoClient

# Connect to MongoDB
MONGO_URL = 'mongodb+srv://sonaligupta:Q8CXEyuEsQK2N611@cluster0.xhtq2pr.mongodb.net/'
client = MongoClient(MONGO_URL)
db = client['books']
collection = db['publisher_details']

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
        # print(document)
        books.append(document)
    print(len(books))

if __name__ == '__main__':
    # Specify the folder path
    # folder_path = 'ebook_jsons'
    # save_books_to_mongodb(folder_path)
    get_books_by_publisher("O'Reilly Media")
