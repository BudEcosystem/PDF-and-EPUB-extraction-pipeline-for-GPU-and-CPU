import sys

sys.path.append("pdf_extraction_pipeline")
from dotenv import load_dotenv
import os
import re
import uuid
from utils import get_mongo_collection


book_set_2 = get_mongo_collection("book_set_2")
book_images = get_mongo_collection("book_images")
table_collection = get_mongo_collection("table_collection")
book_details = get_mongo_collection("book_details")
figure_caption = get_mongo_collection("figure_caption")
publaynet_pages = get_mongo_collection("publaynet_pages")
table_bank_pages = get_mongo_collection("table_bank_pages")
mfd_pages = get_mongo_collection("mfd_pages")
nougat_pages = get_mongo_collection("nougat_pages")
latex_pages = get_mongo_collection("latex_pages")
other_pages = get_mongo_collection("other_pages")
text_pages = get_mongo_collection("text_pages")
ptm_pages = get_mongo_collection("ptm_pages")


# delete wrong tables
def delete_wrong_tables():
    # for document in book_set_2.find():
    for document in book_set_2.find({
        "bookId": {
            "$in": [
                '13d86bfa32fd4fbe88023aa54d7c2bbc',
                '14a51624d9e943df986d4823c9b72936',
                '54bb794c5f1843e8bb35bf6208b54bfa',
                'a7a681f35c5540c9aff28e556f0f2e6f'
            ]
        }
    }):
        print(f"BookId :: {document['bookId']}")
        for page in document["pages"]:
            page_num = page["page_num"]
            print(f"page number :: {page_num}")
            text = page["text"]
            if text:
                # Search for the pattern '{{table:someid}}'
                matches = re.findall(r"\{\{table:(\w+)\}\}", text)
                print(f"Matches :: {matches}")
                if matches:
                    for table_id in matches:
                        # table_id = int(table_id_match, 16)
                        print(f"Table ID :: {table_id}")
                        table_present = False
                        table_details = table_collection.find_one({"tableId": table_id})
                        if table_details:
                            table_data = table_details.get("table_data", {})
                            if table_data:
                                book_set_2.update_one(
                                    {
                                        "_id": document["_id"],
                                        "pages.page_num": page_num,
                                    },
                                    {"$addToSet": {"pages.$.tables": table_data}},
                                )
                                table_present = True
                        # Check if the tableId is present in the tables array
                        if not table_present and (
                            not any(
                                table.get("id", "") == table_id
                                for table in page["tables"]
                            )
                        ):
                            # Replace the pattern with an empty string
                            text = text.replace(f"{{{{table:{table_id}}}}}", "")
                            print(text)
                            # Update the text field in the current page
                            book_set_2.update_one(
                                {"_id": document["_id"], "pages.page_num": page_num},
                                {
                                    "$set": {"pages.$.text": text},
                                },
                            )
    print("Text replacement completed.")


# remove page_num from every page object and add key id to every page object
def remove_page_num_and_add_page_id():
    for document in book_set_2_new.find():
        for page in document["pages"]:
            if "page_num" in page:
                page.pop("page_num")
            page["id"] = uuid.uuid4().hex
        book_set_2_new.update_one(
            {"_id": document["_id"]}, {"$set": {"pages": document["pages"]}}
        )
    print("page_num deleted and id field added in every page.")


# check if there is any duplicate document exist between previously extracted books and newly extracted books
# if no same book is not present in previously extracted collection then add that book
def check_duplicate_and_add_books():
    for document in book_set_2_new.find():
        book = document["book"]
        print(book)
        dup_document = book_set_old.find_one({"book": book})
        if not dup_document:
            # If no duplicate is found, add the document to book_set_old
            book_set_old.insert_one(document)
            print(f"Added book {book} to book_set_old")
        else:
            print(f"This book {book} already present")


# remove book's other document, like other_pages, nougat_pages, nougat_done etc once book is completly extracted
def remove_matching_documents():
    # Iterate over documents in collection1
    for document in book_set_2_new.find():
        book_id = document.get("bookId")

        # Search for matching documents in book_other_pages and book_other pages done collection
        other_pages = book_other_pages.find_one({"bookId": book_id})
        if other_pages:
            book_other_pages.delete_one({"_id": other_pages["_id"]})
            print(f"Removed document from book_other_pages with bookId: {book_id}")

        other_pages_done = book_other_pages_done.find_one({"bookId": book_id})
        if other_pages_done:
            book_other_pages_done.delete_one({"_id": other_pages_done["_id"]})
            print(f"Removed document from book_other_pages_done with bookId: {book_id}")

        # Search for matching documents in nougat_pages and nougat_done collection
        nougat_page_document = nougat_pages.find_one({"bookId": book_id})
        if nougat_page_document:
            nougat_pages.delete_one({"_id": nougat_page_document["_id"]})
            print(f"Removed document from nougat_pages with bookId: {book_id}")

        nougat_document = nougat_done.find_one({"bookId": book_id})
        if nougat_document:
            nougat_done.delete_one({"_id": nougat_document["_id"]})
            print(f"Removed document from nougat_done with bookId: {book_id}")

        # Search for matching documents in latex_page and latex_pages done
        latex_document = latex_pages.find_one({"bookId": book_id})
        if latex_document:
            latex_pages.delete_one({"_id": latex_document["_id"]})
            print(f"Removed document from latex_pages with bookId: {book_id}")

        latex_document_done = latex_pages_done.find_one({"bookId": book_id})
        if latex_document_done:
            latex_pages_done.delete_one({"_id": latex_document_done["_id"]})
            print(f"Removed document from latex_pages_done with bookId: {book_id}")

        # Search for matching documents in publaynet_pages and publaynet_done
        publaynet_document = publaynet_book_job_details.find_one({"bookId": book_id})
        if publaynet_document:
            publaynet_book_job_details.delete_one({"_id": publaynet_document["_id"]})
            print(
                f"Removed document from publaynet_book_job_details with bookId: {book_id}"
            )

        publaynet_document_done = publaynet_done.find_one({"bookId": book_id})
        if publaynet_document_done:
            publaynet_done.delete_one({"_id": publaynet_document_done["_id"]})
            print(f"Removed document from publaynet_done with bookId: {book_id}")

        # Search for matching documents in table_bank_pages and table_bank_done
        tableBank_document = table_bank_book_job_details.find_one({"bookId": book_id})
        if tableBank_document:
            table_bank_book_job_details.delete_one({"_id": tableBank_document["_id"]})
            print(
                f"Removed document from table_bank_book_job_details with bookId: {book_id}"
            )

        tableBank_document_done = table_bank_done.find_one({"bookId": book_id})
        if tableBank_document_done:
            table_bank_done.delete_one({"_id": tableBank_document_done["_id"]})
            print(f"Removed document from table_bank_done with bookId: {book_id}")

        # Search for matching documents in mfd_pages and mfd_done
        mfd_document = mfd_book_job_details.find_one({"bookId": book_id})
        if mfd_document:
            mfd_book_job_details.delete_one({"_id": mfd_document["_id"]})
            print(f"Removed document from mfd_book_job_details with bookId: {book_id}")

        mfd_document_done = mfd_done.find_one({"bookId": book_id})
        if mfd_document_done:
            mfd_done.delete_one({"_id": mfd_document_done["_id"]})
            print(f"Removed document from mfd_done with bookId: {book_id}")

        # search for matching documents in figure_caption collection
        figcap_document = figure_caption.find_one({"bookId": book_id})
        if figcap_document:
            figure_caption.delete_one({"_id": figcap_document["_id"]})
            print(f"Removed document from figure_caption with bookId: {book_id}")

        # search for matching documents in table_caption collection
        table_document = table_collection.find_one({"bookId": book_id})
        if table_document:
            table_collection.delete_one({"_id": table_document["_id"]})
            print(f"Removed document from table_document with bookId: {book_id}")
    print("Processing completed.")


def clean_db(bookId):
    book_images.delete_many({"bookId": bookId})
    nougat_pages.delete_many({"bookId": bookId})
    latex_pages.delete_many({"bookId": bookId})
    other_pages.delete_many({"bookId": bookId})
    text_pages.delete_many({"bookId": bookId})
    # publaynet_pages.delete_many({"bookId": bookId})
    # table_bank_pages.delete_many({"bookId": bookId})
    # mfd_pages.delete_many({"bookId": bookId})
    # figure_caption.delete_many({"bookId": bookId})
    ptm_pages.delete_many({"bookId": bookId})
    table_collection.delete_many({"bookId": bookId})


if __name__ == "__main__":
    delete_wrong_tables()

    #############################################
    # To clean db for fully extracted books
    #############################################
    # for book in book_details.find({"status": "extracted"}):
    for book in book_details.find({
        "bookId": {
            "$in": [
                '13d86bfa32fd4fbe88023aa54d7c2bbc',
                '14a51624d9e943df986d4823c9b72936',
                '54bb794c5f1843e8bb35bf6208b54bfa',
                'a7a681f35c5540c9aff28e556f0f2e6f'
            ]
        }
    }):
        bookId = book["bookId"]
        clean_db(bookId)

    #############################################
    # To clean db for partially extracted books
    #############################################
    # for book in book_details.find({"bookId": {"$in": [
    #     "54283de6ba64477fbfcdd024b75977d1",
    #     "83c9ed88aedd484da3cf022983fc1a65",
    #     "a1d7d2b5eeb04805b6c284a9de3706e7",
    #     "6692536cac904bd394e27301623dc35a"
    # ]}}):
    #     bookId = book["bookId"]
    #     if book["status"] == "processing":
    #         doc_keys = book.keys()
    #         desired_keys = ["_id", "bookId", "book", "status"]
    #         book_details.update_one(
    #             {"bookId": bookId},
    #             {
    #                 "$set": {
    #                     "status": "not_extracted"
    #                 },
    #                 "$unset": {key: "" for key in doc_keys if key not in desired_keys}
    #             }
    #         )
    #     clean_db(bookId)
    pass
