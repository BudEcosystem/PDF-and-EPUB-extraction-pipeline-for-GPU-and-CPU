from utils import get_all_books_names,mongo_init

db=mongo_init('epub_testing')
extracted_books=db.extracted_books

not_extracted=[]
extracted=[]

books = get_all_books_names('bud-datalake', 'Books/Oct29-oreilly-2/')
print(len(books))
for book_number, book in enumerate(books, start=0):
    already_extracted=extracted_books.find_one({"book":book})
    if not already_extracted:
        # print(f"Processing book {book_number} , {book}")
        not_extracted.append(book)
        # get_book_data(book)
        # # get_book_data(book)
    else:
        # print(f'this {book} already extracted')
        extracted.append(book)

print("total orielly extracted",len(extracted))
print("total orielly not extracted",len(not_extracted))




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