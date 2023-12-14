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
from pdf_producer import error_queue, other_pages_queue, latex_ocr_queue, nougat_pdf_queue, book_completion_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel
# sonali : added utility function
from utils import create_image_str, get_mongo_collection

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()

figure_caption = get_mongo_collection('figure_caption')
table_bank_done = get_mongo_collection('table_bank_done')
publaynet_done = get_mongo_collection('publaynet_done')
mfd_done = get_mongo_collection('mfd_done')
publaynet_book_job_details = get_mongo_collection('publaynet_book_job_details')
table_bank_book_job_details = get_mongo_collection('table_bank_book_job_details')
mfd_book_job_details = get_mongo_collection('mfd_book_job_details')
figure_caption = get_mongo_collection('figure_caption')
nougat_done = get_mongo_collection('nougat_done')
book_other_pages_done = get_mongo_collection('book_other_pages_done')
latex_pages_done = get_mongo_collection('latex_pages_done')


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
                        # sonali: added int() to convert page_num to int
                        page_num = int(page["page_num"])
                        result_array = page.get("result", [])
                        image_path = page.get("image_path", "")
                        for result in result_array:
                            result["image_path"] = image_path

                        # Initialize an empty list for the page if it doesn't exist in the dictionary
                        if page_num not in page_results:
                            page_results[page_num] = []
                        page_results[page_num].extend(result_array)

            # sonali : are we doing this just to get image path of a particular page ?
            for page_num, result_array in page_results.items():
                if not result_array:
                    print(page_num)
                    print(bookId)
                    # Fetch image_path from publeynet_collection for the given page_num
                    publeynet_document = publaynet_book_job_details.find_one({"bookId": bookId})
                    for page in publeynet_document['pages']:
                        if page['page_num'] == page_num:
                            image_path = page['image_path']
                            result = {
                                "image_path": image_path,
                            }
                            page_results[page_num].append(result)
            
            #divide pages into nougat_pages, other_pages and latex_ocr pages and send them into queues
            # sonali : initialise image_str dict
            image_str_dict = {}
            nougat_pages = []
            other_pages=[]
            latex_ocr_pages=[]
            for page_num_str, results in page_results.items():
                page_num=int(page_num_str)
                image_path = results[0]['image_path']
                # sonali : send image_str alongwith image_path
                if page_num not in image_str_dict:
                    image_str_dict[page_num] = create_image_str(image_path)
                image_str = image_str_dict[page_num]
                process_page_data = {
                    "page_num": page_num,
                    "results": results,
                    "image_path": image_path,
                    "image_str": image_str,
                    "bookname": bookname,
                    "bookId": bookId
                }
                np, op, lp = process_page(process_page_data)
                # np, op, lp = process_page(results,image_path, page_num, bookId, bookname)
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
                    for page_data in other_pages:
                        other_pages_queue('other_pages_queue', page_data, total_other_pages, bookname, bookId)
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
                    for page in latex_ocr_pages:
                        latex_ocr_queue('latex_ocr_queue', page, total_latex_pages, bookname, bookId)
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
                    nougat_pdf_queue('nougat_pdf_queue', nougat_pages, bookname, bookId)
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


# def process_page(results, image_path, page_num, bookId, bookname):
def process_page(process_page_data):
    page_num = process_page_data["page_num"]
    results = process_page_data["results"]
    image_path = process_page_data["image_path"]
    image_str = process_page_data["image_str"]
    bookname = process_page_data["bookname"]
    bookId = process_page_data["bookId"]

    nougat_pages = []
    other_pages = []
    latex_ocr_pages = []
    try:
        # sonali : how does this condition identify nougat pages ?
        # if page has text or title but no table and figure still we will get x_1 and y_1 -- isn't it ?
        # block = {'x_1': 218.2264862060547, 'y_1': 246.16607666015625, 'x_2': 1293.7288818359375, 'y_2': 395.0075988769531, 'type': 'Title'}
        # is it for pages where no results are found ?
        if not any('x_1' in block and 'y_1' in block for block in results):
            nougat_pages.append({
                "image_path": image_path,
                "page_num": page_num,
                "bookname": bookname,
                "bookId": bookId,
                "image_str": image_str
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

                    # sonali : how did we decide on these values ?
                    # and if constant value then move to .env or global variable
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
                "bookId": bookId,
                "image_str": image_str
            })
        elif any(block['type'] == "Equation" for block in results):
            latex_ocr_pages.append({
                "results": results,
                "image_path": image_path,
                "bookname": bookname,
                "bookId": bookId,
                "page_num": page_num,
                "pdFigCap": pdFigCap,
                "image_str": image_str
            })
        else:
            other_pages.append({
                "results":results,
                "image_path":image_path,
                "bookname":bookname,
                "bookId":bookId,
                "page_num":page_num,
                "pdFigCap":pdFigCap,
                "image_str": image_str
            })   
    except Exception as e:
        error={"consumer":"ptm_consumer","page":{page_num}, "error":{str(e)}, "line_number": {traceback.extract_tb(e.__traceback__)[-1].lineno}}
        print(error)
        error_queue('error_queue',bookname, bookId, error)
    return nougat_pages, other_pages, latex_ocr_pages



def consume_ptm_completion_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)

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