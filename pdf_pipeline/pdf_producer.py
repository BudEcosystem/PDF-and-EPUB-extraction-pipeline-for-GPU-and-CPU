# pylint: disable=all
# type: ignore
import sys

sys.path.append("pdf_extraction_pipeline")
import json
from dotenv import load_dotenv
import os
import boto3
from utils import (
    get_mongo_collection,
    generate_unique_id,
    get_rabbitmq_connection,
    get_channel,
)


load_dotenv()

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_REGION"]

# Create an S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

bucket_name = os.environ["AWS_BUCKET_NAME"]
folder_name = os.environ["BOOK_FOLDER_NAME"]

book_details = get_mongo_collection("book_details")
sequentially_extracted_books = get_mongo_collection("sequentially_extracted_books")


def send_to_queue(queue_name, data):
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    queue_msg = None
    if queue_name == "pdf_processing_queue":
        queue_msg = get_pdf_processing_queue_msg(data)
    elif queue_name == "pdfigcapx_queue":
        queue_msg = get_pdfigcap_queue_msg(data)
    elif queue_name in [
        "publaynet_queue",
        "table_bank_queue",
        "mfd_queue",
        "ptm_queue",
    ]:
        queue_msg = get_layout_queue_msg(data)
    elif queue_name == "check_ptm_completion_queue":
        queue_msg = get_check_ptm_queue_msg(data)
    elif queue_name in [
        "other_pages_queue",
        "latex_ocr_queue",
        "nougat_queue",
        "text_pages_queue",
    ]:
        queue_msg = get_extraction_queue_msg(data)
    elif queue_name == "bud_table_extraction_queue":
        queue_msg = get_bud_table_extraction_queue_msg(data)
    elif queue_name == "book_completion_queue":
        queue_msg = get_book_completion_queue_msg(data)
    if queue_msg is not None:
        channel.queue_declare(queue=queue_name)
        channel.basic_publish(
            exchange="", routing_key=queue_name, body=json.dumps(queue_msg)
        )
        print(f" [x] Sent data to {queue_name}")
    else:
        error_queue_name = "error_queue"
        queue_msg = {"for_queue": queue_name, "data": data}
        channel.queue_declare(queue=error_queue_name)
        channel.basic_publish(
            exchange="", routing_key=error_queue_name, body=json.dumps(queue_msg)
        )
        print(f" [x] Sent msg to {error_queue_name}")
    connection.close()


def get_all_books_names(bucket_name, folder_name):
    """
    Get all books names from aws s3 bucket

    Args:
        bucket_name (str): The name of the AWS S3 bucket.
        folder_name (str): The name of the folder within the bucket.

    Returns:
        list: A list of dictionaries representing the contents (objects) in the specified folder.
    """
    contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    pdf_file_names = [obj["Key"] for obj in contents.get("Contents", [])]
    book_names = [file_name.split("/")[1] for file_name in pdf_file_names]
    return book_names


def get_pdf_processing_queue_msg(book):
    book["_id"] = str(book["_id"])
    pdf_message = {"queue": "pdf_processing_queue", "book": book}
    return pdf_message


def get_layout_queue_msg(data):
    queue_message = {
        "image_path": data["image_path"],
        "page_num": data["page_num"],
        "bookId": data["bookId"],
        # "split_path": data["split_path"],
        "image_str": data["image_str"],
    }
    return queue_message


def get_pdfigcap_queue_msg(data):
    queue_msg = {
        "book_path": data["split_path"],
        "bookId": data["bookId"],
        "from_page": data["from_page"],
        "to_page": data["to_page"],
    }
    return queue_msg


def get_check_ptm_queue_msg(data):
    queue_msg = {
        "bookId": data["bookId"],
        # "split_path": data["split_path"],
        "page_num": data.get("page_num", None),
    }
    return queue_msg


def get_book_completion_queue_msg(bookId):
    queue_msg = {"bookId": bookId}
    return queue_msg


def get_extraction_queue_msg(data):
    queue_msg = {
        "results": data.get("results", None),
        "bookId": data["bookId"],
        "page_num": data.get("page_num"),
        "image_path": data.get("image_path"),
        "is_figure_present": data.get("is_figure_present", None),
        "split_id": data.get("split_id", None),
    }
    return queue_msg


def get_bud_table_extraction_queue_msg(data):
    queue_msg = {
        "tableId": data["tableId"],
        "data": data["data"],
        "page_num": data["page_num"],
        "bookId": data["bookId"],
    }
    return queue_msg


def error_queue(book_path, bookId, error):
    queue_name = "error_queue"
    connection = get_rabbitmq_connection()
    channel = get_channel(connection)
    error_queue = {"book_path": book_path, "bookId": bookId, "error": error}
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(
        exchange="", routing_key=queue_name, body=json.dumps(error_queue)
    )
    del error["consumer_message"]
    print(f" [x] Sent {error} sent to {queue_name}")
    connection.close()


def store_book_details():
    #     books= get_all_books_names(bucket_name, folder_name + '/')
    #     books=books[127:]
    #     books=["Guide to Competitive Programming - Antti Laaksonen.pdf","Guide to Computer Network Security - Joseph Migga Kizza.pdf","Guide to Discrete Mathematics - Gerard O'Regan.pdf","Guide to Scientific Computing in C++ - Joe Pitt-Francis- Jonathan Whiteley.pdf","Handbook of Consumer Finance Research - Jing Jian Xiao.pdf","Handbook of Disaster Research - Havidan Rodriguez- Enrico L Quarantelli- Russell Dynes.pdf","Handbook of Evolutionary Research in Archaeology - Anna Marie Prentiss.pdf","Handbook of LGBT Elders - Debra A Harley- Pamela B Teaster.pdf","Handbook of Marriage and the Family - Gary W Peterson- Kevin R Bush.pdf","Handbook of Quantitative Criminology - Alex R Piquero- David Weisburd.pdf","Handbook of the Life Course - Jeylan T Mortimer- Michael J Shanahan.pdf","International Business Management - Kamal Fatehi- Jeongho Choi.pdf","International Humanitarian Action - Hans-Joachim Heintze- Pierre Thielbörger.pdf","International Perspectives on Psychotherapy - Stefan G Hofmann.pdf","International Trade Theory and Policy - Giancarlo Gandolfo.pdf","Internet of Things From Hype to Reality - Ammar Rayes- Samer Salam.pdf","Introduction to Data Science - Laura Igual- Santi Seguí.pdf","Introduction to Deep Learning - Sandro Skansi.pdf","Introduction to Electronic Commerce and Social Commerce - Efraim Turban- Judy Whiteside- David King- Jon Outland.pdf","Introduction to Evolutionary Computing - AE Eiben- JE Smith.pdf","Introduction to Formal Philosophy - Sven Ove Hansson- Vincent F Hendricks.pdf","Introduction to General Relativity - Cosimo Bambi.pdf","Introduction to Law - Jaap Hage- Antonia Waltermann- Bram Akkermans.pdf",
    # "Introduction to Mathematica® for Physicists - Andrey Grozin.pdf","Introduction to Parallel Computing - Roman Trobec- Boštjan Slivnik- Patricio Bulić- Borut Robič.pdf","Introduction to Partial Differential Equations - David Borthwick.pdf","Introduction to Programming with Fortran - Ian Chivers- Jane Sleightholme.pdf","Introduction to Smooth Manifolds - John Lee.pdf",
    # "Introduction to Statistics and Data Analysis  - Christian Heumann- Michael Schomaker-  Shalabh.pdf","Introduction to Time Series and Forecasting - Peter J Brockwell- Richard A Davis.pdf"
    # ,"Introductory Quantum Mechanics - Paul R Berman.pdf","Introductory Statistics with R - Peter Dalgaard.pdf","Introductory Time Series with R - Paul SP Cowpertwait- Andrew V Metcalfe.pdf",
    # "Knowledge Management - Klaus North- Gita Kumta.pdf","Language Across the Curriculum & CLIL in English as an Additional Language (EAL) Contexts - Angel MY Lin.pdf"]
    # books = ["output_3.pdf"]
    # books = [
    #     "Writing for Publication - Mary Renck Jalongo- Olivia N Saracho.pdf",
    #     "Witnessing Torture - Alexandra S Moore- Elizabeth Swanson.pdf",
    #     "Understanding Statistics Using R - Randall Schumacker- Sara Tomek.pdf",
    # ]
    books = get_all_books_names(bucket_name, folder_name + "/")
    print(len(books))
    for book in books:
        doc = book_details.find_one({"book": book})
        doc2 = sequentially_extracted_books.find_one({"book": book})
        if not doc and not doc2:
            book_data = {
                "bookId": generate_unique_id(),
                "book": book,
                "status": "not_extracted",
            }
            book_details.insert_one(book_data)


if __name__ == "__main__":
    try:
        # # # store all books from aws to book_details collection before running
        # store_book_details()
        books = book_details.find({"status": "not_extracted"}).limit(10)
        # books = book_details.find({"bookId": {"$in": ["14a51624d9e943df986d4823c9b72936", "61776d86a35a49acb26a4f69b9d65b88"]}})
        for book in books:
            if book["book"].endswith(".pdf"):
                print(book["book"])
                send_to_queue("pdf_processing_queue", book)
            else:
                error_queue("", book["bookId"], "File extension not .pdf")
                print("skipping this book as it not a pdf file")

    except KeyboardInterrupt:
        pass
