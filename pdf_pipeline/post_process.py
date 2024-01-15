
from dotenv import load_dotenv
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
    for book in book_details.find({"status": "post_process"}):
        bookId = book["bookId"]
        document = book_set_2.find_one({"bookId": bookId})
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


def clean_post_process():
    for book in book_details.find({"status": "post_process"}):
        bookId = book["bookId"]
        clean_db(bookId)
        book_details.update_one(
            {"bookId": bookId}, 
            {"$set": {"status": "extracted"}})


def clean_processing():
    for book in book_details.find({"status": "processing"}):
        bookId = book["bookId"]
        if book["status"] == "processing":
            doc_keys = book.keys()
            desired_keys = ["_id", "bookId", "book", "status"]
            book_details.update_one(
                {"bookId": bookId},
                {
                    "$set": {
                        "status": "not_extracted"
                    },
                    "$unset": {key: "" for key in doc_keys if key not in desired_keys}
                }
            )
        clean_db(bookId)


if __name__ == "__main__":
    delete_wrong_tables()

    #############################################
    # To clean db for fully extracted books
    #############################################
    clean_post_process()

    #############################################
    # To clean db for partially extracted books
    #############################################
    # clean_processing()
    pass
