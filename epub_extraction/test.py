from utils import mongo_init

db = mongo_init("book_set_2")
book_set_2 = db.book_set_2
# document = book_set_2.find_one({"bookId": "bf85d5518de24d6fb02c78b155cbae83"})
# if document:
#     for page in document["pages"]:
#         if "page_equations" in page:
#             page["equations"] = page.pop("page_equations")
#     book_set_2.update_one(
#         {"_id": document["_id"]}, {"$set": {"pages": document["pages"]}}
#     )

for document in book_set_2.find({}):
    for page in document["pages"]:
        if "page_equations" in page:
            page["equations"] = page.pop("page_equations")

    # Update the document in the book_set_2
    print(document["book"])
    book_set_2.update_one(
        {"_id": document["_id"]}, {"$set": {"pages": document["pages"]}}
    )
