# pylint: disable=all
# type: ignore
import pymongo

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
table_collection=db.table_collection
book_collection=db.book_set_2_new

# Iterate over documents in the table_data collection
for table_doc in table_collection.find():
    bookId = table_doc['bookId']

    # Find the corresponding document in the book_collection
    book_doc = book_collection.find_one({"bookId": bookId})
    if book_doc:
        # Iterate over pages in both collections
        for table_page, book_page in zip(table_doc['pages'], book_doc['pages']):
            page_num = table_page['page_num']

            # Add page_tables from table_collection to the tables array of book_collection document
            book_page['tables'] = table_page.get('page_tables', [])

        # Update the book_collection document
        book_collection.update_one({"_id": book_doc["_id"]}, {"$set": {"pages": book_doc["pages"]}})
