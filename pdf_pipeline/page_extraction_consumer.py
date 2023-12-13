# pylint: disable=all
# type: ignore
from dotenv import load_dotenv
import traceback
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
import os
import pymongo
import uuid
import boto3
import json
import traceback
from pdf_producer import other_pages_queue, latex_ocr_queue, error_queue, nougat_pdf_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()



# latex_ocr_model = LatexOCR()
# Configure AWS credentials
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
db = client.bookssssss
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


def extract_pages(ch, method, properties, body):
    try:
        message = json.loads(body)
        # print(message)
        page_results=message['book_pages']
        bookname = message["bookname"]
        bookId = message["bookId"]
        print("BookID : ", bookId)
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
        if total_other_pages>0:
            for page_num, page_result in enumerate(other_pages):
                other_pages_queue('other_pages_queue',page_result, total_other_pages,page_num, bookname, bookId)
            print("other pages sent, sending latex_ocr pages...")
        else:
            book_other_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"latex pages Done"})

        # send latex_ocr_pages to latex_ocr_queue
        total_latex_pages=len(latex_ocr_pages)
        if total_latex_pages>0:
            for page_num, page_result in enumerate(latex_ocr_pages):
                # upload page_result images to s3
                # replace in image_path in page_result with s3 url
                image_path = page_result['image_path']
                new_image_path = upload_to_s3(image_path)
                page_result['image_path'] = new_image_path
                latex_ocr_queue('latex_ocr_queue',page_result,total_latex_pages,page_num, bookname, bookId)
            print("latex_ocr pages sent, sending nougat pages .....")
        else:
            latex_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"latex pages Done"})

        # send nougat_pages to nougat_queue   
        total_nougat_pages = len(nougat_pages)
        if total_nougat_pages>0:
            nougat_pdf_queue('nougat_pdf_queue',nougat_pages,bookname, bookId)
        else:
            nougat_done.insert_one({"bookId":bookId,"book":bookname,"status":"nougat pages Done"})
    except Exception as e:
        error={"consumer":"page_extraction","error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
        print(error)
        error_queue('error_queue',bookname, bookId, error)    
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

# def process_page_result(results, page_num, bookId,bookname,nougat_pages,other_pages,latex_ocr_pages):
#     image_path=results[0]['image_path']
#     process_page(results,image_path, page_num, bookId,bookname, nougat_pages, other_pages,latex_ocr_pages)

def process_page(results, image_path, page_num, bookId, bookname):
    nougat_pages = []
    other_pages = []
    latex_ocr_pages = []
    try:
        print(type(results))
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
        error={"page":{page_num}, "error":{str(e)}, "line_number": {traceback.extract_tb(e.__traceback__)[-1].lineno}}
        print(error)
        error_queue('error_queue',bookname, bookId, error)
    return nougat_pages, other_pages, latex_ocr_pages


def consume_page_extraction_queue():
    try:    
        channel_number = channel.channel_number
        print(f"Channel number: {channel_number}")
        # Declare the queue
        channel.queue_declare(queue='page_extraction_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='page_extraction_queue', on_message_callback=extract_pages)

        print(' [*] Waiting for messages on page_extraction_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass

   
if __name__ == "__main__":
    try:
        consume_page_extraction_queue()
        # s3_url = upload_to_s3("../flowChart.png")
        # print(s3_url)
    except KeyboardInterrupt:
        pass