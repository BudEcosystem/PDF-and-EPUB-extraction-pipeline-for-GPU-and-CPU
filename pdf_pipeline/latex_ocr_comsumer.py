# pylint: disable=all
# type: ignore
import numpy as np
from dotenv import load_dotenv
import subprocess
import pytesseract
import traceback
import pytz
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
from pdf_producer import book_completion_queue
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
latex_pages=db.latex_pages
latex_pages_done=db.latex_pages_done

def extract_latex_pages():
    try:
        # message = json.loads(body)
        # print(message)
        # pages_results=message['latex_pages']
        pages_results= [{'results': [{'x_1': 218.18670654296875, 'y_1': 2216.549072265625, 'x_2': 1616.80078125, 'y_2': 2477.67333984375, 'type': 'Text', 'image_path': '/home/azureuser/prakash/output_2/page_6.jpg'}, {'x_1': 225.19680786132812, 'y_1': 234.2555694580078, 'x_2': 463.64044189453125, 'y_2': 294.0711669921875, 'type': 'Title', 'image_path': '/home/azureuser/prakash/output_2/page_6.jpg'}, {'x_1': 219.01083374023438, 'y_1': 340.3992614746094, 'x_2': 1616.5682373046875, 'y_2': 824.836669921875, 'type': 'Text', 'image_path': '/home/azureuser/prakash/output_2/page_6.jpg'}, {'x_1': 336.001708984375, 'y_1': 1307.069580078125, 'x_2': 1502.7852783203125, 'y_2': 2186.296142578125, 'type': 'Table', 'image_path': '/home/azureuser/prakash/output_2/page_6.jpg'}, {'x_1': 479.0030212402344, 'y_1': 1573.6387939453125, 'x_2': 1318.937255859375, 'y_2': 2007.815673828125, 'type': 'Equation', 'image_path': '/home/azureuser/prakash/output_2/page_6.jpg'}, {'x_1': 316.9840546697039, 'y_1': 1304.6366366366365, 'x_2': 1505.6742596810936, 'y_2': 2184.1201201201197, 'type': 'Figure', 'caption': 'Fig. 1.9 The Tinn-R text editor. Each bracket style has a distinctive colour. Under Options->Main->Editor, the font size can be increased. Under Options->Main->Application->R,you can specify the path for R. Select the Rgui.exe file in the directory C:\\Program Files\\R\\R-2.7.1\\bin (assuming default installation settings). Adjust the R directory if you use a differentR version. This option allows sending blocks of code directly to R by highlighting code andclicking one of the icons above the file name'}], 'image_path': '/home/azureuser/prakash/output_2/page_6.jpg', 'bookname': 'output_2.pdf', 'bookId': '76874rf', 'page_num': 5, 'pdFigCap': True}, {'results': [{'x_1': 324.66717529296875, 'y_1': 1841.6123046875, 'x_2': 1491.63134765625, 'y_2': 2350.459716796875, 'type': 'Table', 'image_path': '/home/azureuser/prakash/output_2/page_8.jpg'}, {'x_1': 244.31036376953125, 'y_1': 1320.2064208984375, 'x_2': 1337.520751953125, 'y_2': 1376.8568115234375, 'type': 'Equation', 'image_path': '/home/azureuser/prakash/output_2/page_8.jpg'}, {'x_1': 316.9840546697039, 'y_1': 1833.9939939939939, 'x_2': 1505.6742596810936, 'y_2': 2350.846846846847, 'type': 'Figure', 'caption': 'Fig. 1.11 R is waiting for more code, as an incomplete command has been typed. Either addthe remaining code or press ‘‘escape’’ to abort the boxplot command'}], 'image_path': '/home/azureuser/prakash/output_2/page_8.jpg', 'bookname': 'output_2.pdf', 'bookId': '76874rf', 'page_num': 7, 'pdFigCap': True},{'results': [{'x_1': 233.15745544433594, 'y_1': 1967.5355224609375, 'x_2': 1593.453857421875, 'y_2': 2311.07177734375, 'type': 'Text', 'image_path': '/home/azureuser/prakash/output_2/page_9.jpg'}, {'x_1': 331.02178955078125, 'y_1': 552.8185424804688, 'x_2': 1505.067626953125, 'y_2': 1723.0263671875, 'type': 'Table', 'image_path': '/home/azureuser/prakash/output_2/page_9.jpg'}, {'x_1': 594.6592407226562, 'y_1': 1437.848876953125, 'x_2': 1198.1834716796875, 'y_2': 1478.45703125, 'type': 'Equation', 'image_path': '/home/azureuser/prakash/output_2/page_9.jpg'}, {'x_1': 316.9840546697039, 'y_1': 562.7027027027027, 'x_2': 1505.6742596810936, 'y_2': 1717.2852852852852, 'type': 'Figure', 'caption': 'Fig. 1.12 The window that is obtained by clicking Help->Html help from the help menu in R.Search Engine & Keywords allows searching for functions, commands, and keywords. Youwill need to switch off any pop-up blockers'}], 'image_path': '/home/azureuser/prakash/output_2/page_9.jpg', 'bookname': 'output_2.pdf', 'bookId': '76874rf', 'page_num': 8, 'pdFigCap': True},{'results': [{'x_1': 217.73353576660156, 'y_1': 1330.966796875, 'x_2': 1598.81005859375, 'y_2': 1802.5146484375, 'type': 'Text', 'image_path': '/home/azureuser/prakash/output_2/page_14.jpg'}, {'x_1': 347.414794921875, 'y_1': 264.9136657714844, 'x_2': 1503.9525146484375, 'y_2': 1096.74951171875, 'type': 'Table', 'image_path': '/home/azureuser/prakash/output_2/page_14.jpg'}, {'x_1': 392.6797180175781, 'y_1': 542.591796875, 'x_2': 1124.5792236328125, 'y_2': 834.2861938476562, 'type': 'Equation', 'image_path': '/home/azureuser/prakash/output_2/page_14.jpg'}, {'x_1': 316.9840546697039, 'y_1': 241.75375375375373, 'x_2': 1505.6742596810936, 'y_2': 1121.2372372372372, 'type': 'Figure', 'caption': 'Fig. 1.15 Our Tinn-R code. Note that we copied the code up to, and including, the final roundbracket. We should have dragged the mouse one line lower to include the hidden enter that willexecute the xyplot command'}], 'image_path': '/home/azureuser/prakash/output_2/page_14.jpg', 'bookname': 'output_2.pdf', 'bookId': '76874rf', 'page_num': 13, 'pdFigCap': True}]
        # bookname = message["bookname"]
        bookId = "8677667"
        for page in pages_results:
            page_obj= process_pages(page)
            document=latex_pages.find_one({'bookId':bookId})
            if document:
                latex_pages.update_one({"_id":document["_id"]}, {"$push": {"pages": page_obj}})
            else:
                new_book_document = {
                    "bookId": bookId,
                    "book": bookname,  
                    "pages": [page_obj]
                }
                latex_pages.insert_one(new_book_document)
        latex_pages_done.insert_one({"bookId":bookId,"book":bookname,"status":"latex pages Done"})
        # book_completion_queue("book_completion_queue",bookname, bookId)
    except Exception as e:
        print(e)
    finally:
        print("ack received")
        # ch.basic_ack(delivery_tag=method.delivery_tag)

    


def process_pages(page):
    try:
        page_tables=[]
        page_figures=[]
        page_equations=[]
        results = page.get("results", [])
        image_path = page.get("image_path", "")
        pdFigCap = page.get("pdFigCap", False)
        page_num = page.get("page_num", "")
        page_content= sort_text_blocks_and_extract_data(results, image_path,page_tables,page_figures,page_equations, pdFigCap)
        page_obj={
            "page_num":page_num,
            "content":page_content,
            "tables":page_tables,
            "figures":page_figures,
            "equations":page_equations
        }
        return page_obj
    except Exception as e:
        print("error while page",e)

def sort_text_blocks_and_extract_data(blocks, imagepath, page_tables, page_figures, page_equations, pdFigCap):
    try:
        print("hello")
        sorted_blocks = sorted(blocks, key=lambda block: (block['y_1'] + block['y_2']) / 2)
        print(sorted_blocks)
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
    except Exception as e:
        print('error while sorting,',e)


@timeit
def process_table(imagepath, output, page_tables):
    try:
        output=process_book_page(imagepath,page_tables, output)
        return output
    except Exception as e:
        print("error procwss",e)

@timeit
def process_figure(figure_block, imagepath, output, page_figures):
    try:
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
    except Exception as e:
        print("error while figure",e)  

@timeit
def process_publeynet_figure(figure_block, imagepath, prev_block, next_block, output, page_figures):
    print("publeynbeje")
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
    try:
        textId=uuid.uuid4().hex
        cropped_image_path = crop_image(text_block,imagepath, textId)
        image =Image.open(cropped_image_path)
        text = pytesseract.image_to_string(image)
        output+=text
        if os.path.exists(cropped_image_path):
            os.remove(cropped_image_path)
        return output
    except Exception as e:
        print("error while process text",e)  
    
    
    

@timeit
def process_title(title_block,imagepath, output):
    try:
        titleId=uuid.uuid4().hex
        cropped_image_path = crop_image(title_block,imagepath, titleId)
        #extraction of text from cropped image using pytesseract
        image =Image.open(cropped_image_path)
        text = pytesseract.image_to_string(image)
        output+=text
        if os.path.exists(cropped_image_path):
            os.remove(cropped_image_path)
        return output
    except Exception as e:
        print("error while process title",e)  
    

@timeit
def process_list(list_block,imagepath, output):
    try:
        listId=uuid.uuid4().hex
        cropped_image_path = crop_image(list_block,imagepath, listId)
        image =Image.open(cropped_image_path)
        text = pytesseract.image_to_string(image)
        output+=text
        if os.path.exists(cropped_image_path):
            os.remove(cropped_image_path)
        return output
    except Exception as e:
        print("error while process list",e)  
    
@timeit
def process_equation(equation_block, imagepath, output, page_equations):
    try:
        print("hello")
        equationId=uuid.uuid4().hex
        equation_image_path = crop_image(equation_block,imagepath, equationId)
        print(equation_image_path)
        output += f"{{{{equation:{equationId}}}}}"
        img = Image.open(equation_image_path)
        print(img)
        # latex_text="458jbdfhfbv"
        try:
            model = LatexOCR()
            latex_text= model(img)
            print(latex_text)
        except Exception as e:
            print(e)
        print("hellosdshhf")
        text_to_speech=latext_to_text_to_speech(latex_text)
        page_equations.append(
            {'id': equationId, 'text':latex_text, 'text_to_speech':text_to_speech}
            )
        if os.path.exists(equation_image_path):
            os.remove(equation_image_path)
        return output
    except Exception as e:
        print("error while equation",e)  
 

@timeit
def latext_to_text_to_speech(text):
    try:
        text = "${}$".format(text.lstrip('\\'))
        text_to_speech = latex_to_text(text)
        return text_to_speech
    except Exception as e:
        print('error while text to speech',e)


def consume_latex_ocr_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='latex_ocr_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='latex_ocr_queue', on_message_callback=extract_latex_pages)

        print(' [*] Waiting for messages on latec_ocr_queue To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
   


if __name__ == "__main__":
    try:
        # consume_latex_ocr_queue()      
        extract_latex_pages()
    except KeyboardInterrupt:
        pass