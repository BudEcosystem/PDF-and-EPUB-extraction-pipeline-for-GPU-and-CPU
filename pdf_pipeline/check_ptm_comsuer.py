# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import traceback
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import os
import pymongo
import json
import uuid
import boto3
from pdf_producer import error_queue, other_pages_queue, latex_ocr_queue, nougat_pdf_queue, book_completion_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']

# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']

s3_base_url = os.getenv("S3_BASE_URL")
s3_folder_path_latex = os.getenv("S3_FOLDER_PATH_LATEX")


client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.book_set_2
figure_caption = db.figure_caption
table_bank_done=db.table_bank_done
publaynet_done=db.publaynet_done
mfd_done=db.mfd_done
publaynet_book_job_details=db.publaynet_book_job_details
table_bank_book_job_details=db.table_bank_book_job_details
mfd_book_job_details=db.mfd_book_job_details
figure_caption = db.figure_caption
nougat_done=db.nougat_done
book_other_pages_done=db.book_other_pages_done
latex_pages_done=db.latex_pages_done


def upload_to_s3(filepath):
    """
    Uploads a file to S3
    Args:
        filepath (str): The path to the file to upload.
        bookname (str): The name of the book.
        bookId (str): The ID of the book.
        page_num (int): The page number of the page to upload.
    Returns:
        str: The URL of the uploaded file.
    """
    try:
        # Generate a random filename
        filename = uuid.uuid4().hex
        # Get the file extension
        extension = filepath.split('.')[-1]
        # Create the new filename
        key = f"{s3_folder_path_latex}/{filename}.{extension}"
        # Upload the file to S3
        s3.upload_file(
            Filename=filepath, 
            Bucket=bucket_name, 
            Key=key
        )
        # Get the URL of the uploaded file
        url = f"{s3_base_url}/{key}"
        return url
    except Exception as e:
        print(e)
        return None


def check_ptm_status(ch, method, properties, body):
    try:
        print("hello pmt called")
        message = json.loads(body)
        bookname = message["bookname"]
        bookId = message["bookId"]

        publeynet_done_document = publaynet_done.find_one({"bookId": bookId})
        table_done_document = table_bank_done.find_one({"bookId": bookId})
        mfd_done_document = mfd_done.find_one({"bookId": bookId})
        pdFigCap = figure_caption.find_one({"bookId": bookId})

        if publeynet_done_document and table_done_document and mfd_done_document and pdFigCap:
            collections = [publaynet_book_job_details, table_bank_book_job_details, mfd_book_job_details]
            page_results = {}

            for collection in collections:
                documents = collection.find({"bookId": bookId})  # Use find() to get multiple documents
                for document in documents:
                    for page in document.get("pages", []):
                        page_num = page["page_num"]
                        result_array = page.get("result", [])
                        image_path = page.get("image_path", "")
                        for result in result_array:
                            result["image_path"] = image_path

                        # Initialize an empty list for the page if it doesn't exist in the dictionary
                        if page_num not in page_results:
                            page_results[page_num] = []
                        page_results[page_num].extend(result_array)

            for page_num, result_array in page_results.items():
                if not result_array:
                    print(page_num)
                    print(bookId)
                    # Fetch image_path from publeynet_collection for the given page_num
                    publeynet_document = publaynet_book_job_details.find_one({"bookId": bookId})
                    for page in publeynet_document['pages']:
                        if page['page_num']==page_num:
                            image_path=page['image_path']
                            page_results[page_num].append({"image_path": image_path})
            
            #divide pages into nougat_pages, other_pages and latex_ocr pages and send them into queues
            nougat_pages = []
            other_pages=[]
            latex_ocr_pages=[]
            for page_num_str, results in page_results.items():
                page_num=int(page_num_str)
                image_path = results[0]['image_path']
                # process_page_result(results, page_num, bookId, bookname, nougat_pages, other_pages,latex_ocr_pages) 
                np, op, lp = process_page(results,image_path, page_num, bookId, bookname)
                nougat_pages.extend(np)
                other_pages.extend(op)
                latex_ocr_pages.extend(lp)

            # //send other page to other_pages_queue
            total_other_pages=len(other_pages)
            other_pages_doc = book_other_pages_done.find_one({"bookId": bookId})
            if other_pages_doc:
                print("other_pages already exist for this book")
                book_completion_queue('book_completion_queue', bookname, bookId)
            else:
                if total_other_pages>0:
                    for page_num, page_result in enumerate(other_pages):
                        other_pages_queue('other_pages_queue',page_result, total_other_pages,page_num, bookname, bookId)
                    print("other pages sent, sending latex_ocr pages...")
                else:
                    book_other_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"latex pages Done"})
                    book_completion_queue('book_completion_queue', bookname, bookId)

            # # send latex_ocr_pages to latex_ocr_queue
            total_latex_pages=len(latex_ocr_pages)
            latex_pages_doc = latex_pages_done.find_one({"bookId": bookId})
            if latex_pages_doc:
                print("latex pages already exist for this book")
                book_completion_queue('book_completion_queue', bookname, bookId)
            else:
                if total_latex_pages>0:
                    for page_num, page_result in enumerate(latex_ocr_pages):
                        image_path = page_result['image_path']
                        new_image_path = upload_to_s3(image_path)
                        page_result['image_path'] = new_image_path
                        latex_ocr_queue('latex_ocr_queue',page_result,total_latex_pages,page_num, bookname, bookId)
                    print("latex_ocr pages sent, sending nougat pages .....")
                else:
                    latex_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"latex pages Done"})
                    book_completion_queue('book_completion_queue', bookname, bookId)

            # send nougat_pages to nougat_queue   
            total_nougat_pages = len(nougat_pages)
            nougat_pages_doc = nougat_done.find_one({"bookId": bookId})
            if nougat_pages_doc:
                print("nougat pages already exist for this book")
                book_completion_queue('book_completion_queue', bookname, bookId)
            else:
                if total_nougat_pages>0:
                    nougat_pdf_queue('nougat_pdf_queue',nougat_pages,bookname, bookId)
                else:
                    nougat_done.insert_one({"bookId":bookId,"book":bookname,"status":"nougat pages Done"})
                    book_completion_queue('book_completion_queue', bookname, bookId)

        else:
            print("not yet completed")
    except Exception as e:
        error = {"consumer":"check_ptm_consumer","consumer_message":message,"error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue',bookname, bookId,error)      
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def process_page(results, image_path, page_num, bookId, bookname):
    nougat_pages = []
    other_pages = []
    latex_ocr_pages = []
    try:
        print(type(results))
        if not any('x_1' in block and 'y_1' in block for block in results):
            nougat_pages.append({
                "image_path": image_path,
                "page_num": page_num,
                "bookname": bookname,
                "bookId": bookId
            })
            return nougat_pages, other_pages, latex_ocr_pages

        pdFigCap = False
        # Check the status in the figure_caption collection
        document_status = figure_caption.find_one({"bookId": bookId, "status": "success"})
        if document_status:
            pdFigCap = True
            results = [block for block in results if "type" in block and block['type'] != 'Figure']
            figures_block = []
            for page in document_status.get("pages", []):
                if page.get("page_num") == page_num + 1:
                    figure_bbox_values = page.get("figure_bbox")
                    caption_text = page.get('caption_text')
                    caption = ''.join(caption_text)

                    old_page_width = 439
                    old_page_height = 666
                    new_page_width = 1831
                    new_page_height = 2776

                    width_scale = new_page_width / old_page_width
                    height_scale = new_page_height / old_page_height

                    x1, y1, x2, y2 = figure_bbox_values

                    x1 = x1 * width_scale
                    y1 = y1 * height_scale
                    x2 = x2 * width_scale
                    y2 = y2 * height_scale

                    x2 = x1 + x2
                    y2 = y1 + y2

                    figure_block = {
                        "x_1": x1,
                        "y_1": y1,
                        "x_2": x2,
                        "y_2": y2,
                        "type": "Figure",
                        "caption": caption
                    }
                    figures_block.append(figure_block)

            if figures_block:
                results.extend(figures_block)

        if not results or not any(block['type'] in ["Table", "Figure"] for block in results):
            nougat_pages.append({
                "image_path":image_path,
                "page_num": page_num,
                "bookname": bookname, 
                "bookId": bookId
            })
        elif any(block['type'] == "Equation" for block in results):
            latex_ocr_pages.append({
                "results": results,
                "image_path": image_path,
                "bookname": bookname,
                "bookId": bookId,
                "page_num": page_num,
                "pdFigCap": pdFigCap
            })
        else:
            other_pages.append({
                "results":results,
                "image_path":image_path,
                "bookname":bookname,
                "bookId":bookId,
                "page_num":page_num,
                "pdFigCap":pdFigCap
            })   
    except Exception as e:
        error={"consumer":"ptm_consumer","page":{page_num}, "error":{str(e)}, "line_number": {traceback.extract_tb(e.__traceback__)[-1].lineno}}
        print(error)
        error_queue('error_queue',bookname, bookId, error)
    return nougat_pages, other_pages, latex_ocr_pages



def consume_ptm_completion_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='check_ptm_completion_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='check_ptm_completion_queue', on_message_callback=check_ptm_status)

        print(' [*] Waiting for messages on check_ptm_completion_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()
   


if __name__ == "__main__":
    try:
        consume_ptm_completion_queue()      
    except KeyboardInterrupt:
        pass