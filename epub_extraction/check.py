from utils import get_all_books_names,mongo_init

db=mongo_init('epub_testing')
extracted_books=db.extracted_books
publishers=db.publishers

not_extracted=[]
extracted=[]

# books = get_all_books_names('bud-datalake', 'Books/Oct29-oreilly-2/')
# print(len(books))
# for book_number, book in enumerate(books, start=0):
#     already_extracted=extracted_books.find_one({"book":book})
#     if not already_extracted:
#         # print(f"Processing book {book_number} , {book}")
#         not_extracted.append(book)
#         # get_book_data(book)
#         # # get_book_data(book)
#     else:
#         # print(f'this {book} already extracted')
#         extracted.append(book)

# print("total orielly extracted",len(extracted))
# print("total orielly not extracted",len(not_extracted))
s3_keys=[]
missing_s3Keys=[]
for book in publishers.find():
    if 'publishers' in book and book['publishers'] and book['publishers'][0].startswith("O'Reilly"):
        if 's3_key' in book:
            bookname=book['s3_key'].split('/')[-2]
            s3_keys.append(bookname)
        else:
            missing_s3Keys.append(book['title'])
print(f'total books with s3_keys {len(s3_keys)}')
print(f'total books with s3_keys {len(missing_s3Keys)}')

# books=['Introduction to Machine Learning with Python (9781449369880)']
# for book in books:
#     already_extracted=extracted_books.find_one({"book":book})
#     if not already_extracted:
#         get_book_data(book)




# import csv

# books = get_all_books_names('bud-datalake', 'Books/Oct29-oreilly-2/')
# print(len(books))
# csv_file_path = '/home/bud-data-extraction/datapipeline/pdf_extraction_pipeline/epub_extraction/oreilly.csv'

# book_ids = []
# with open(csv_file_path, 'r') as file:
#     reader = csv.reader(file)
#     # Skip the header if it exists
#     next(reader, None)
#     for row in reader:
#         # Assuming the book_id is in the first (and only) column
#         book_id = row[0]
#         book_ids.append(book_id)

# print(len(book_ids))