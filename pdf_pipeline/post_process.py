
import re
import pymongo
from utils import get_mongo_collection
from pdf_pipeline.pdf_producer import send_to_queue

libgen_data = get_mongo_collection("libgen_data")
book_set_2 = get_mongo_collection("libgen_data_2")
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
book_set_2_dup = get_mongo_collection("book_set_2_dup")


# delete wrong tables
def delete_wrong_tables():
    error_ids = []
    for book in book_details.find({"status": "post_process"}).limit(40):
        bookId = book["bookId"]
        for document in book_set_2.find({"bookId": bookId}):
            print(f"BookId :: {document['bookId']}")
            for page in document["pages"]:
                if bookId in error_ids:
                    continue
                page_num = page["page_num"]
                # print(f"page number :: {page_num}")
                text = page["text"]
                if text:
                    # Search for the pattern '{{table:someid}}'
                    matches = re.findall(r"\{\{table:(\w+)\}\}", text)
                    # print(f"Matches :: {matches}")
                    if matches:
                        for table_id in matches:
                            # table_id = int(table_id_match, 16)
                            # print(f"Table ID :: {table_id}")
                            table_present = False
                            table_details = table_collection.find_one({"tableId": table_id})
                            if table_details:
                                table_data = table_details.get("table_data", {})
                                if table_data:
                                    try:
                                        book_set_2.update_one(
                                            {
                                                "_id": document["_id"],
                                                "pages.page_num": page_num,
                                            },
                                            {"$addToSet": {"pages.$.tables": table_data}},
                                        )
                                    except pymongo.errors.WriteError:
                                        print("continuing pymongo write error")
                                        error_ids.append(bookId)
                                        continue
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
                                # print(text)
                                # Update the text field in the current page
                                book_set_2.update_one(
                                    {"_id": document["_id"], "pages.page_num": page_num},
                                    {
                                        "$set": {"pages.$.text": text},
                                    },
                                )
        if bookId in error_ids:
            continue
        clean_db(bookId)
        book_details.update_one(
            {"bookId": bookId}, 
            {"$set": {"status": "extracted"}})
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
    for book in book_details.find({"status": "post_process"}).limit(40):
        bookId = book["bookId"]
        print(bookId)
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


def duplicate_collection():
    # Find all documents in the source collection
    documents = list(book_set_2.find())
    # Insert found documents into the destination collection
    if documents:
        book_set_2_dup.insert_many(documents)


def remove_duplicates():
    # Aggregate duplicates
    pipeline = [
        {"$group": {
            "_id": {"bookId": "$bookId", "split_order": {"$ifNull": ["$split_order", None]}},
            "duplicates": {"$addToSet": "$_id"},
            "count": {"$sum": 1}
            }
        },
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(book_set_2.aggregate(pipeline))

    # Remove duplicates, keeping one document for each group
    for doc in duplicates:
        doc['duplicates'].pop(0)  # Keep one document, remove the rest
        book_set_2.delete_many({"_id": {"$in": doc['duplicates']}})


def get_nougat_not_extracted():
    book_details = get_mongo_collection('book_details')
    nougat_pages = get_mongo_collection('nougat_pages')
    for book in book_details.find({"status": "processing", "type": "pdf"}):
        bookId = book["bookId"]
        bd = book_details.find_one({"bookId": bookId})
        np = nougat_pages.find_one({"bookId": bookId})
        ns = bd.get('nougat_splits', {})
        if np:
            nps = np.get('pages', [])
            ne = [num for num in bd['num_nougat_pages'] if str(num) not in nps]
            if ne:
                processing_nougat_split = bd["processing_nougat_split"]
                last_split_id = processing_nougat_split[-1]
                for page_num in ne:
                    ns[last_split_id].append({
                        "bookId": bookId,
                        "page_num": page_num,
                        "image_path": f"/src/libgen_data/{bookId}/pages/page_{page_num}.jpg"
                    })
                book_details.update_one(
                    {"bookId": bookId},
                    {"$set": {"nougat_splits": ns},
                    "$pull": {"processing_nougat_split": last_split_id}}
                )
                send_to_queue("nougat_queue", {
                    "bookId": bookId,
                    "page_num": ne[0],
                    "image_path": f"/src/libgen_data/{bookId}/pages/page_{ne[0]}.jpg"
                })
                print(f"BookId : {bookId}")
                print(f"not extracted : {ne}")


def calculate_time_taken_for_books():
    from datetime import datetime
    st_time = None
    end_time = None
    count = 0
    total_pages = 0
    datetime_format = "%d-%m-%Y %H:%M:%S"
    for book in book_details.find({"time_taken": {"$exists": True}}):
        # print(count)
        # print(book["bookId"])
        total_pages += book["num_pages"]
        if st_time is None:
            st_time = book["start_time"]
        if end_time is None:
            end_time = book["end_time"]
        sto = datetime.strptime(st_time, datetime_format)
        eto = datetime.strptime(end_time, datetime_format)
        bsto = datetime.strptime(book["start_time"], datetime_format)
        beto = datetime.strptime(book["end_time"], datetime_format)
        if bsto <= sto:
            st_time = bsto.strftime(datetime_format)
        if beto >= eto:
            end_time = beto.strftime(datetime_format)
        count += 1
    total_sec = (datetime.strptime(end_time, datetime_format) - datetime.strptime(st_time, datetime_format)).total_seconds()
    print("total books extracted >>> ", count)
    print("total pages extracted >>> ", total_pages)
    print("total time taken >>> ", total_sec)
    print("books extracted per min >>> ", (total_sec/60)/count)
    print("time taken to extract 1 page >>> ", total_pages/total_sec)

if __name__ == "__main__":
    # get_nougat_not_extracted()
    remove_duplicates()
    for i in range(0,5):
        delete_wrong_tables()

    #############################################
    # To clean db for fully extracted books
    #############################################
    # clean_post_process()

    #############################################
    # To clean db for partially extracted books
    #############################################
    # clean_processing()
    pass
