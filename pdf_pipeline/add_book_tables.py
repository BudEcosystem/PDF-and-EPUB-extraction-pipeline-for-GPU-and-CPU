import pymongo
from dotenv import load_dotenv
import os
import re
import uuid
load_dotenv()
client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db1 = client.book_set_2
book_other_pages=db1.book_other_pages
nougat_pages=db1.nougat_pages
nougat_done=db1.nougat_done
book_other_pages_done=db1.book_other_pages_done
latex_pages=db1.latex_pages
latex_pages_done=db1.latex_pages_done
table_bank_done = db1.table_bank_done
publaynet_done = db1.publaynet_done
mfd_done = db1.mfd_done
publaynet_book_job_details = db1.publaynet_book_job_details
table_bank_book_job_details = db1.table_bank_book_job_details
mfd_book_job_details = db1.mfd_book_job_details
figure_caption = db1.figure_caption

table_collection=db1.table_collection
book_set_2_new = db1.book_set_2_new


#second database
db2=client.books
book_set_old=db2.book_set_2_new


# delete wrong tables
def delete_wrong_tables():
    for document in book_set_2_new.find():
        print("jdhdfjn")
        for page in document['pages']:
            page_num = page['page_num']
            text = page['text']
            print('yehjk')
            # Search for the pattern '{{table:someid}}'
            matches = re.findall(r'\{\{table:(\w+)\}\}', text)
            print(matches)
            for table_id_match in matches:
                table_id = int(table_id_match, 16)
                # Check if the tableId is present in the tables array
                if not any(int(table.get('id'), 16) == table_id for table in page['tables']):
                    # Replace the pattern with an empty string
                    text = text.replace(f'{{{{table:{table_id_match}}}}}', '')
                    print(text)
            # Update the text field in the current page
            book_set_2_new.update_one(
                {'_id': document['_id'], 'pages.page_num': page_num},
                {'$set': {'pages.$.text': text}}
            )
    print("Text replacement completed.")

#remove page_num from every page object and add key id to every page object
def remove_page_num_and_add_page_id():
    for document in book_set_2_new.find():
        for page in document['pages']:
            if 'page_num' in page:
                page.pop('page_num')
            page['id'] = uuid.uuid4().hex
        book_set_2_new.update_one({'_id': document['_id']}, {'$set': {'pages': document['pages']}})
    print("page_num deleted and id field added in every page.")

#check if there is any duplicate document exist between previously extracted books and newly extracted books
# if no same book is not present in previously extracted collection then add that book
def check_duplicate_and_add_books():
    for document in book_set_2_new.find():
        book=document['book']
        print(book)
        dup_document=book_set_old.find_one({"book":book})
        if not dup_document:
            # If no duplicate is found, add the document to book_set_old
            book_set_old.insert_one(document)
            print(f"Added book {book} to book_set_old")
        else:
            print(f'This book {book} already present')

#remove book's other document, like other_pages, nougat_pages, nougat_done etc once book is completly extracted
def remove_matching_documents():
    # Iterate over documents in collection1
    for document in book_set_2_new.find():
        book_id = document.get('bookId')

        #Search for matching documents in book_other_pages and book_other pages done collection
        other_pages = book_other_pages.find_one({'bookId': book_id})
        if other_pages:
            book_other_pages.delete_one({'_id': other_pages['_id']})
            print(f"Removed document from book_other_pages with bookId: {book_id}")
        
        other_pages_done = book_other_pages_done.find_one({'bookId': book_id})
        if other_pages_done:
            book_other_pages_done.delete_one({'_id': other_pages_done['_id']})
            print(f"Removed document from book_other_pages_done with bookId: {book_id}")

        #Search for matching documents in nougat_pages and nougat_done collection
        nougat_page_document= nougat_pages.find_one({'bookId': book_id})
        if nougat_page_document:
            nougat_pages.delete_one({'_id':  nougat_page_document['_id']})
            print(f"Removed document from nougat_pages with bookId: {book_id}")
        
        nougat_document = nougat_done.find_one({'bookId': book_id})
        if nougat_document:
            nougat_done.delete_one({'_id': nougat_document['_id']})
            print(f"Removed document from nougat_done with bookId: {book_id}")
        
        #Search for matching documents in latex_page and latex_pages done
        latex_document = latex_pages.find_one({'bookId': book_id})
        if latex_document:
            latex_pages.delete_one({'_id':  latex_document['_id']})
            print(f"Removed document from latex_pages with bookId: {book_id}")
        
        latex_document_done = latex_pages_done.find_one({'bookId': book_id})
        if latex_document_done:
            latex_pages_done.delete_one({'_id':  latex_document_done['_id']})
            print(f"Removed document from latex_pages_done with bookId: {book_id}")
        
        #Search for matching documents in publaynet_pages and publaynet_done
        publaynet_document = publaynet_book_job_details.find_one({'bookId': book_id})
        if publaynet_document:
            publaynet_book_job_details.delete_one({'_id': publaynet_document['_id']})
            print(f"Removed document from publaynet_book_job_details with bookId: {book_id}")
        
        publaynet_document_done = publaynet_done.find_one({'bookId': book_id})
        if publaynet_document_done:
            publaynet_done.delete_one({'_id':  publaynet_document_done['_id']})
            print(f"Removed document from publaynet_done with bookId: {book_id}")

        #Search for matching documents in table_bank_pages and table_bank_done
        tableBank_document = table_bank_book_job_details.find_one({'bookId': book_id})
        if tableBank_document:
            table_bank_book_job_details.delete_one({'_id': tableBank_document['_id']})
            print(f"Removed document from table_bank_book_job_details with bookId: {book_id}")
        
        tableBank_document_done = table_bank_done.find_one({'bookId': book_id})
        if tableBank_document_done:
            table_bank_done.delete_one({'_id':tableBank_document_done['_id']})
            print(f"Removed document from table_bank_done with bookId: {book_id}")
        
        #Search for matching documents in mfd_pages and mfd_done
        mfd_document = mfd_book_job_details.find_one({'bookId': book_id})
        if mfd_document:
            mfd_book_job_details.delete_one({'_id':mfd_document['_id']})
            print(f"Removed document from mfd_book_job_details with bookId: {book_id}")
        
        mfd_document_done = mfd_done.find_one({'bookId': book_id})
        if mfd_document_done:
            mfd_done.delete_one({'_id':mfd_document_done['_id']})
            print(f"Removed document from mfd_done with bookId: {book_id}")
        
        #search for matching documents in figure_caption collection
        figcap_document = figure_caption.find_one({'bookId': book_id})
        if figcap_document:
            figure_caption.delete_one({'_id':figcap_document['_id']})
            print(f"Removed document from figure_caption with bookId: {book_id}")
        
        #search for matching documents in table_caption collection
        table_document = table_collection.find_one({'bookId': book_id})
        if table_document:
            table_collection.delete_one({'_id':table_document['_id']})
            print(f"Removed document from table_document with bookId: {book_id}")
    print("Processing completed.")











# delete_wrong_tables()
# remove_page_num_and_add_page_id()
# check_duplicate_and_add_books()
    
# remove_matching_documents()


