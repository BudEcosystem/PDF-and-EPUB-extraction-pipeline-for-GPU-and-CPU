import numpy as np
from dotenv import load_dotenv
import subprocess
import pytesseract
import traceback
import sys
sys.path.append("pdf_extraction_pipeline/code")
sys.path.append("pdf_extraction_pipeline")
from PIL import Image
import os
import PyPDF2
import img2pdf
import fitz
import shutil
import boto3
import re
import cv2
import pymongo
from urllib.parse import urlparse
import urllib
import uuid
from latext import latex_to_text
from pix2tex.cli import LatexOCR
from PyPDF2 import PdfReader
from tablecaption import process_book_page
from utils import timeit, crop_image
import pika
import json
from pdf_producer import nougat_queue, book_completion_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel

connection = get_rabbitmq_connection()
channel = get_channel(connection)

load_dotenv()



latex_ocr_model = LatexOCR()
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


client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
bookdata = db.book_set_2_new
error_collection = db.error_collection
figure_caption = db.figure_caption
table_bank_done=db.table_bank_done
publaynet_done=db.publaynet_done
mfd_done=db.mfd_done
publaynet_book_job_details=db.publaynet_book_job_details
table_bank_book_job_details=db.table_bank_book_job_details
mfd_book_job_details=db.mfd_book_job_details
book_other_pages=db.book_other_pages
book_other_pages_done=db.book_other_pages_done

def check_ptm_status(ch, method, properties, body):
    try:
        message = json.loads(body)
        job = message['job']
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
                document = collection.find_one({"bookId": bookId})
                if document:
                    for page in document.get("pages", []):
                        page_num = page["page_num"]
                        result_array = page.get("result", [])
                        image_path = page.get("image_path", "")
                        for result in result_array:
                            result["image_path"] = image_path

                        # Initialize an empty list for the page if it doesn't exist in the dictionary
                        if page_num not in page_results:
                            page_results[page_num] = []

                        # Append the results to the page-specific list
                        page_results[page_num].extend(result_array)

            nougat_pages = []
            for page_num, results in page_results.items():
                page_object = process_page_result(results, page_num, bookId, bookname, nougat_pages)
                if page_object is not None:
                    book_document = book_other_pages.find_one({"bookId": bookId})
                    if book_document:
                        book_other_pages.update_one({"_id": book_document["_id"]}, {"$push": {"pages": page_object}})
                    else:
                        new_book_document = {
                            "bookId": bookId,
                            "book": bookname,  
                            "pages": [page_object]
                        }
                        book_other_pages.insert_one(new_book_document)

            book_other_pages_done.insert_one({"bookId": bookId, "book": bookname, "status": "book_other_pages_done"})
            book_completion_queue("book_completion_queue", bookname, bookId) 
            total_nougat_pages = len(nougat_pages)
            for page_num, page in enumerate(nougat_pages):
                nougat_queue('nougat_queue', page['image_path'], total_nougat_pages, page['page_num'], page_num,
                             page['bookname'], page['bookId'])
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print("not yet completed")
    except Exception as e:
        print(e)



def process_page_result(results, page_num, bookId,bookname,nougat_pages):
    image_path=results[0]['image_path']
    page_content,page_tables, page_figures, page_equations=process_page(results,image_path, page_num, bookId,bookname, nougat_pages)
    if all(value is None for value in [page_content, page_tables, page_figures, page_equations]):
        return None
    pageId= uuid.uuid4().hex
    page_obj={
        "page_num":page_num,
        "text":page_content,
        "tables":page_tables,
        "figures":page_figures,
        "equations":page_equations
    }
    return page_obj


def process_page(results, image_path, page_num, bookId,bookname,nougat_pages):
    try:
        pdFigCap = False

    # Check the status in the figure_caption collection
        document_status = figure_caption.find_one({"bookId": bookId, "status": "success"})

        if document_status:
            pdFigCap = True
            results = [block for block in results if block['type'] != 'Figure']
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

        page_tables = []
        page_figures = []
        page_equations = []

        if not results or not any(block['type'] in ["Table", "Figure"] for block in results):
            nougat_pages.append({
                "image_path":image_path,
                "page_num": page_num,
                "bookname": bookname, 
                "bookId": bookId
            })
            return None,None,None,None
       
        if not pdFigCap and len(results)==1 and results[0]['type']=='Figure':
            x1, y1, x2, y2 = results[0]['x_1'], results[0]['y_1'], results[0]['x_2'], results[0]['y_2']
            img = cv2.imread(image_path)
            figure_bbox = img[int(y1):int(y2), int(x1):int(x2)]
            figureId=uuid.uuid4().hex
            figure_image_path = f"wrong{figureId}.png"
            cv2.imwrite(figure_image_path,figure_bbox) 
            image = Image.open(figure_image_path)
            page_content=''
            if image.height>1500:
                text = pytesseract.image_to_string(image)
                page_content = re.sub(r'\s+', ' ', text)
            else:
                # figure_url = upload_to_aws_s3(imagepath,figureId)
                page_figures.append({
                    "id":figureId,
                    "url":'figure_url',
                    "caption": ""
                })
            if os.path.exists(figure_image_path):
                os.remove(figure_image_path)
            return page_content, page_tables,page_figures,page_equations
            

        page_content = sort_text_blocks_and_extract_data(results, image_path, page_tables, page_figures, page_equations, pdFigCap)

        return page_content, page_tables, page_figures, page_equations
    except Exception as e:
        print(f"An error occurred while processing {bookname}, page {page_num}: {str(e)}, line_numbe {traceback.extract_tb(e.__traceback__)[-1].lineno}")
        return [],[],[],[]

def sort_text_blocks_and_extract_data(blocks, imagepath, page_tables, page_figures, page_equations, pdFigCap):
    sorted_blocks = sorted(blocks, key=lambda block: (block['y_1'] + block['y_2']) / 2)
    output = ""
    prev_block = None
    next_block = None
    for i, block in enumerate(sorted_blocks): 
        if i > 0:
            prev_block = sorted_blocks[i - 1]
        if i < len(sorted_blocks) - 1:
            next_block = sorted_blocks[i + 1]  
        if block['type'] == "Table":
            output = process_table(imagepath, output, page_tables)
        elif block['type'] == "Figure":
            if pdFigCap:
                output = process_figure(block, imagepath, output, page_figures)
            else:
                output=process_publeynet_figure(block, imagepath, prev_block, next_block, output, page_figures)  
        elif block['type'] == "Text":
            output = process_text(block, imagepath, output)
        elif block['type'] == "Title":
            output = process_title(block, imagepath, output)
        elif block['type'] == "List":
            output = process_list(block, imagepath, output)
        elif block['type']=='Equation':
            output=process_equation(block, imagepath, output, page_equations)

    page_content = re.sub(r'\s+', ' ', output).strip()
    return page_content

@timeit
def process_table(imagepath, output, page_tables):
    output=process_book_page(imagepath,page_tables, output)
    return output

@timeit
def process_figure(figure_block, imagepath, output, page_figures):
    figureId = uuid.uuid4().hex
    figure_image_path = crop_image(figure_block,imagepath, figureId)
    output += f"{{{{figure:{figureId}}}}}"

    # figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":"figure_url",
        "caption": figure_block['caption']
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

@timeit
def process_publeynet_figure(figure_block, imagepath, prev_block, next_block, output, page_figures):
    caption=""
    figureId = uuid.uuid4().hex
    figure_image_path =crop_image(figure_block,imagepath, figureId)
    print(figure_image_path)
    output += f"{{{{figure:{figureId}}}}}"

    if prev_block:
        prevId=uuid.uuid4().hex
        prev_image_path = crop_image(prev_block,imagepath, prevId)
        #extraction of text from cropped image using pytesseract
        image =Image.open(prev_image_path)
        text = pytesseract.image_to_string(image)
        text = re.sub(r'\s+', ' ', text).strip()
        pattern = r"(Fig\.|Figure)\s+\d+"
        match = re.search(pattern, text)
        if match:
            caption = text
        if os.path.exists(prev_image_path):
            os.remove(prev_image_path)

    if next_block:
        nextId=uuid.uuid4().hex
        next_image_path = crop_image(next_block,imagepath, nextId) 
        #extraction of text from cropped image using pytesseract
        image =Image.open(next_image_path)
        text = pytesseract.image_to_string(image)
        text = re.sub(r'\s+', ' ',text).strip()
        pattern = r"(Fig\.|Figure)\s+\d+"
        match = re.search(pattern, text)
        if match:
            caption = text
        if os.path.exists(next_image_path):
            os.remove(next_image_path)

    # figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":'figure_url',
        "caption":caption
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

@timeit
def process_text(text_block,imagepath, output):
    textId=uuid.uuid4().hex
    cropped_image_path = crop_image(text_block,imagepath, textId)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_title(title_block,imagepath, output):
   
    titleId=uuid.uuid4().hex
    cropped_image_path = crop_image(title_block,imagepath, titleId)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_list(list_block,imagepath, output):
    listId=uuid.uuid4().hex
    cropped_image_path = crop_image(list_block,imagepath, listId)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output


@timeit
def process_equation(equation_block, imagepath, output, page_equations):
    equationId=uuid.uuid4().hex
    equation_image_path = crop_image(equation_block,imagepath, equationId)
    output += f"{{{{equation:{equationId}}}}}"
    img = Image.open(equation_image_path)
    latex_text= latex_ocr_model(img)
    text_to_speech=latext_to_text_to_speech(latex_text)
    page_equations.append(
       {'id': equationId, 'text':latex_text, 'text_to_speech':text_to_speech} 
    )
    if os.path.exists(equation_image_path):
        os.remove(equation_image_path)
    return output
 

@timeit
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text.lstrip('\\'))
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech


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
   


if __name__ == "__main__":
    try:
        consume_ptm_completion_queue()
        
    except KeyboardInterrupt:
        pass