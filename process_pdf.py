import numpy as np
from dotenv import load_dotenv
import subprocess
import pytesseract
import sys
from PIL import Image
sys.path.append("pdf_extraction_pipeline/code")
from FigCap import extract_figure_and_caption
from PIL import Image
import os
import PyPDF2
import img2pdf
import fitz
import shutil
import traceback
import boto3
import re
import cv2
import pymongo
from urllib.parse import urlparse
import urllib
import uuid
from PyPDF2 import PdfReader
from tablecaption import process_book_page
from model_loader import ModelLoader 
from utils import timeit
from latext import latex_to_text
from pix2tex.cli import LatexOCR
load_dotenv()
import os
import signal
import atexit

# Configure AWS credentials
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.books
bookdata = db.book_set_2_new
error_collection = db.error_collection
figure_caption = db.figure_caption
book_layout = db.book_layout
book_progress=db.book_progress
book_number=db.book_number



# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

bucket_name = os.environ['AWS_BUCKET_NAME']
folder_name=os.environ['BOOK_FOLDER_NAME']

def store_book_progress(bookname, page_num, bookId, current_book_number):
    book_progress.delete_many({})
    book_progress.insert_one({"bookId":bookId, "book":bookname, "book_number":current_book_number, "page_num":page_num})
    book_number.delete_many({})

def signal_handler(sig, frame):
    # Capture the progress when the script is interrupted
    if current_bookname and current_page_num is not None:
        store_book_progress(current_bookname, current_page_num, current_bookId,current_book_number)
    exit(0)

def exit_handler():
    # Capture the progress when the script exits normally
    if current_bookname and current_page_num is not None:
        store_book_progress(current_bookname, current_page_num, current_bookId, current_book_number)

atexit.register(exit_handler)
signal.signal(signal.SIGINT, signal_handler)

current_bookname = None
current_page_num = None
current_bookId = None
current_book_number=None

@timeit
def get_all_books_names(bucket_name, folder_name):
  contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
  pdf_file_names = [obj['Key'] for obj in contents.get('Contents', [])]
  book_names = [file_name.split('/')[1] for file_name in pdf_file_names]
  return book_names

# downlads particular book from aws and save it to system and return the bookpath
@timeit
def download_book_from_aws(bookname, bookId):
  try:
    os.makedirs(folder_name, exist_ok=True)
    # Save the PDF with the bookname.pdf in the books folder
    local_path = os.path.join(folder_name, f'{bookname}')
    file_key = f'{folder_name}/{bookname}'
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    pdf_data = response['Body'].read()
    with open(local_path, 'wb') as f:
      f.write(pdf_data)
    return local_path   
  except Exception as e:
    print("An error occurred:", e)
    data = {"bookId":{bookId},"book":{bookname}, "error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
    error_collection.insert_one(data)
    return None

@timeit
def process_book(bookname, start_page, bookId):
    newBookId=uuid.uuid4().hex

    if bookId is None:
        bookId=newBookId

    global current_bookname
    global current_page_num
    global current_bookId
    current_bookId=bookId

    book_folder = bookname.replace('.pdf', '')
    book_path = download_book_from_aws(bookname, bookId)
    if not book_path:
         return 
    #extract figure and figure caption
    get_figure_and_captions(book_path, bookname, bookId)
    os.makedirs(book_folder, exist_ok=True)
    book = PdfReader(book_path)  
    print(bookname)
    num_pages = len(book.pages)
    print(f"{bookname} has total {num_pages} page")
    try:
        for page_num in range(start_page,num_pages):
            current_bookname = bookname
            current_page_num = page_num
            print(page_num)
            page_object, page_layout_info=process_page(page_num, book_path, book_folder, bookname, bookId)

            book_document = bookdata.find_one({"bookId":bookId})
            book_layout_doc = book_layout.find_one({"bookId":bookId})
            if book_document:
                bookdata.update_one({"_id": book_document["_id"]}, {"$push": {"pages": page_object}})
            else:
                new_book_document={
                   "bookId":bookId,
                   "book": bookname,
                   "pages": [page_object] 
                }
                bookdata.insert_one(new_book_document)
            if book_layout_doc:
                book_layout.update_one({"_id":book_layout_doc['_id']}, {"$push":{"pages":page_layout_info}})
            else:
                new_book_layout={
                   "bookId":bookId,
                   "book": bookname,
                   "pages": [page_layout_info] 
                }
                book_layout.insert_one(new_book_layout)
                
        book_progress.delete_many({})
        book_number.delete_many({})
        book_number.insert_one({'book_number':current_book_number})

    except Exception as e:
        data = {"bookId":{bookId},"book":{bookname},"error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
        error_collection.insert_one(data)

    #find document by name replace figure caption with ""
    document = bookdata.find_one({"bookId":bookId})
    
    if document:
        for page in document['pages']:
            for figure in page['figures']:
                caption=figure['caption']
                if caption in page['text']:
                    page['text']=page['text'].replace(caption,'')
            for table in page['tables']:
                caption = table['caption']
                if caption in page['text']:
                    page['text']=page['text'].replace(caption,'')

        try:
            result = bookdata.update_one({'_id': document['_id']}, {'$set': {'pages': document['pages']}})
            if result.modified_count == 1:
                print("Document updated successfully.")
            else:
                print("Document update did not modify any document.")
        except Exception as e:
            print("An error occurred:", str(e))

    #delete the book
    os.remove(book_path)
    shutil.rmtree(book_folder)

#convert pages into images and return all pages data
@timeit
def process_page(page_num, book_path, book_folder, bookname, bookId):
    pdf_book = fitz.open(book_path)
    page_image = pdf_book[page_num]
    book_image = page_image.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
    image_path = os.path.join(book_folder, f'page_{page_num + 1}.jpg')
    book_image.save(image_path)
    page_content,page_tables,page_figures, page_equations,layout_blocks= process_image(image_path, page_num, bookname, bookId, pdf_book)
    pageId= uuid.uuid4().hex
    page_obj={
        "id":pageId,
        "text":page_content,
        "tables":page_tables,
        "figures":page_figures,
        "equations":page_equations
    }
    page_layout_info={
        "id":pageId,
        "layout_info":layout_blocks
    }
    print(page_num, "done")
    os.remove(image_path)
    return page_obj, page_layout_info

#detect layout and return page data
@timeit
def process_image(imagepath, page_num, bookname, bookId, pdf_book):
    try:
        image = cv2.imread(imagepath)
        image = image[..., ::-1]

        publaynet = ModelLoader("PubLayNet")
        tablebank = ModelLoader("TableBank")
        mathformuladetection = ModelLoader("MathFormulaDetection")
        
        publaynet_model = publaynet.model
        tablebank_model = tablebank.model
        mathformuladetection_model = mathformuladetection.model

        publaynet_layout = publaynet_model.detect(image)
        tablebank_layout = tablebank_model.detect(image)
        mathformuladetection_layout = mathformuladetection_model.detect(image)

        pdFigCap=False
        final_layout = []

        for block in publaynet_layout:
            if block.type != "Table":
                final_layout.append(block)

        for block in tablebank_layout:
            if block.type == "Table":
                final_layout.append(block)

        for block in mathformuladetection_layout:
            if block.type == 'Equation':
                final_layout.append(block)
        
        layout_blocks = []
        for item in final_layout:  
            output_item = {
                "x_1": item.block.x_1,
                "y_1": item.block.y_1,
                "x_2": item.block.x_2,
                "y_2": item.block.y_2,
                'type': item.type
            }
            layout_blocks.append(output_item)

        document = figure_caption.find_one({"bookId":bookId})

        if document:
            pdFigCap=True
            layout_blocks = [block for block in layout_blocks if block['type'] != 'Figure']
            figures_block=[]
            for page in document.get("pages", []):
                if page.get("page_num") == page_num+1:
                    figure_bbox_values = page.get("figure_bbox")
                    caption_text = page.get('caption_text')
                    caption =''.join(caption_text)

                    old_page_width=439
                    old_page_height=666
                    new_page_width = 1831
                    new_page_height= 2776

                    width_scale=new_page_width/old_page_width
                    height_scale=new_page_height/old_page_height

                    x1, y1, x2, y2 = figure_bbox_values

                    x1=x1*width_scale
                    y1=y1*height_scale
                    x2=x2*width_scale
                    y2=y2*height_scale

                    x2=x1+x2
                    y2=y1+y2
                    figure_block = {
                         "x_1": x1,
                         "y_1": y1,
                         "x_2": x2,
                         "y_2": y2,
                         "type": "Figure",
                         "caption":caption
                    }
                    figures_block.append(figure_block)
            
            if figures_block:
                layout_blocks.extend(figures_block)
        
        page_tables=[]
        page_figures=[]
        page_equations=[]
        

        # Check if layout_blocks is empty or doesn't contain any "Table" or "Figure" blocks then process the page with nougat
        if not layout_blocks or not any(block['type'] in ["Table", "Figure"] for block in layout_blocks):
            try:
                print("extracting using naugat")
                page_content=extract_text_equation_with_nougat(imagepath, page_equations, page_num,bookname, bookId)
                return page_content, page_tables, page_figures, page_equations, layout_blocks

            except Exception as e:
                    print(f"An error occurred while processing {bookname}, page {page_num} with nougat: {str(e)}")
                    error={"error":str(e),"page_number":page_num, "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
                    document=error_collection.find_one({"bookId":bookId})
                    if document:
                        error_collection.update_one({"_id": document["_id"]}, {"$push": {"pages": error}})
                    else:
                        new_error_doc = {"bookId": bookId, "book": bookname, "error_pages": [error]}
                        error_collection.insert_one(new_error_doc)
                    return "",[],[],[],[]

        
        #extract page content based on their region
        if not pdFigCap and len(layout_blocks)==1 and layout_blocks[0]['type']=='Figure':
            x1, y1, x2, y2 = layout_blocks[0]['x_1'], layout_blocks[0]['y_1'], layout_blocks[0]['x_2'], layout_blocks[0]['y_2']
            img = cv2.imread(imagepath)
            figure_bbox = img[int(y1):int(y2), int(x1):int(x2)]
            figureId=uuid.uuid4().hex
            figure_image_path = f"wrong{figureId}.png"
            cv2.imwrite(figure_image_path,figure_bbox) 
            image = Image.open(figure_image_path)
            page_content=''
            if image.height>1500:
                page = pdf_book[page_num]
                text = page.get_text(sort=True)
                page_content = text = re.sub(r'\s+', ' ', text)
            else:
                figure_url = upload_to_aws_s3(imagepath,figureId)
                page_figures.append({
                    "id":figureId,
                    "url":figure_url,
                    "caption": ""
                })
            if os.path.exists(figure_image_path):
                os.remove(figure_image_path)
            return page_content, page_tables,page_figures,page_equations,layout_blocks
            
        page_content = sort_text_blocks_and_extract_data(layout_blocks,imagepath,page_tables,page_figures,page_equations, pdFigCap)
        #extract equations
        # nougat_extraction = extract_text_equation_with_nougat(imagepath, page_equations, page_num,bookname, bookId)
        return page_content,page_tables,page_figures, page_equations,layout_blocks

    except Exception as e:
        print(f"An error occurred while processing {bookname}, page {page_num}: {str(e)}, line_numbe {traceback.extract_tb(e.__traceback__)[-1].lineno}")
        error={"error":str(e),"page_number":page_num, "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
        document=error_collection.find_one({"bookId":bookId})
        if document:
            error_collection.update_one({"_id": document["_id"]}, {"$push": {"pages": error}})
        else:
            new_error_doc = {"bookId": bookId, "book": bookname, "error_pages": [error]}
            error_collection.insert_one(new_error_doc)
        return "", [],[],[],[]

#sort the layout blocks and return page data 
@timeit
def sort_text_blocks_and_extract_data(blocks, imagepath,page_tables, page_figures, page_equations, pdFigCap):
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
            output = process_table(block, imagepath, output, page_tables)
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

# #extract table and table_caption and return table object {id, data, caption}
@timeit
def process_table(table_block, imagepath, output, page_tables):
    x1, y1, x2, y2 = table_block['x_1'], table_block['y_1'], table_block['x_2'], table_block['y_2']
    # # Load the image
    img = cv2.imread(imagepath)
    # Increase top boundary by 70 pixels
    y1 -= 70
    if y1 < 0:
        y1 = 0
    # Increase left boundary to the image's width
    x1 = 0
    # Increase right boundary by 20 pixels
    x2 += 20
    if x2 > img.shape[1]:
        x2 = img.shape[1]
    # Increase bottom boundary by 20 pixels
    y2 += 20
    if y2 > img.shape[0]:
        y2 = img.shape[0]
    # Crop the specified region
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    # Save the cropped image
    table_image_path ="cropped_table.png"
    cv2.imwrite(table_image_path, cropped_image)
    
    #process table and caption with bud-ocr
    output=process_book_page(table_image_path,page_tables, output)

    if os.path.exists(table_image_path):
        os.remove(table_image_path)
    return output

#extract figure and figure_caption and return figure object {id, figureUrl, caption}
@timeit
def process_figure(figure_block, imagepath, output, page_figures):
    # Process the "Figure" block
    x1, y1, x2, y2 = figure_block['x_1'], figure_block['y_1'], figure_block['x_2'], figure_block['y_2']
    img = cv2.imread(imagepath)
    # Expand the bounding box by 5 pixels on every side
    x1-=5
    y1-=5
    x2+=5
    y2+=5

    # Ensure the coordinates are within the image boundaries
    x1=max(0,x1)
    y1=max(0,y1)
    x2=min(img.shape[1],x2)
    y2=min(img.shape[0],y2)

    #crop the expanded bounding box
    figure_bbox = img[int(y1):int(y2), int(x1):int(x2)]
    figureId=uuid.uuid4().hex
    figure_image_path = f"figure_6{figureId}.png"
    cv2.imwrite(figure_image_path,figure_bbox) 
    output += f"{{{{figure:{figureId}}}}}"

    figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":figure_url,
        "caption": figure_block['caption']
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

@timeit
def process_publeynet_figure(figure_block, imagepath, prev_block, next_block, output, page_figures):
    print("process_figure2 called as pdffig is false")
    caption=""
    # Process the "Figure" block
    x1, y1, x2, y2 = figure_block['x_1'], figure_block['y_1'], figure_block['x_2'], figure_block['y_2']
    # Load the image
    img = cv2.imread(imagepath)
    # Expand the bounding box by 5 pixels on every side
    x1-=5
    y1-=5
    x2+=5
    y2+=5

    # Ensure the coordinates are within the image boundaries
    x1=max(0,x1)
    y1=max(0,y1)
    x2=min(img.shape[1],x2)
    y2=min(img.shape[0],y2)

    #crop the expanded bounding box
    figure_bbox = img[int(y1):int(y2), int(x1):int(x2)]
    figureId=uuid.uuid4().hex
    figure_image_path = f"figure_6{figureId}.png"
    cv2.imwrite(figure_image_path,figure_bbox) 
    output += f"{{{{figure:{figureId}}}}}"

    if prev_block:
        prev_x1, prev_y1, prev_x2, prev_y2 = prev_block['x_1'], prev_block['y_1'], prev_block['x_2'], prev_block['y_2']
        prev_x1 -= 5
        prev_y1 -= 5
        prev_x2 += 5
        prev_y2 += 5
        # Ensure the coordinates are within the image boundaries
        prev_x1 = max(0, prev_x1)
        prev_y1 = max(0, prev_y1)
        prev_x2 = min(img.shape[1], prev_x2)
        prev_y2 = min(img.shape[0], prev_y2)
        # Crop the bounding box for the block before the "Figure" block
        prev_bbox = img[int(prev_y1):int(prev_y2), int(prev_x1):int(prev_x2)]
        # Save the cropped bounding box as an image
        prev_image_path = f"prev_block{figureId}.png"
        cv2.imwrite(prev_image_path, prev_bbox)
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
        next_x1, next_y1, next_x2, next_y2 =next_block['x_1'],next_block['y_1'],next_block['x_2'],next_block['y_2']
         # Expand the bounding box by 5 pixels on every side
        next_x1 -= 5
        next_y1 -= 5
        next_x2 += 5
        next_y2 += 5
        
        # Ensure the coordinates are within the image boundaries
        next_x1 = max(0, next_x1)
        next_y1 = max(0, next_y1)
        next_x2 = min(img.shape[1], next_x2)
        next_y2 = min(img.shape[0], next_y2)
        # Crop the bounding box for the block after the "Figure" block
        next_bbox = img[int(next_y1):int(next_y2), int(next_x1):int(next_x2)]
        # Save the cropped bounding box as an image
        next_image_path = f"next_block_{figureId}.png"
        cv2.imwrite(next_image_path, next_bbox)
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

    figure_url=upload_to_aws_s3(figure_image_path, figureId)
    page_figures.append({
        "id":figureId,
        "url":figure_url,
        "caption":caption
    })
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output    

#extract and return text from text block
@timeit
def process_text(text_block,imagepath, output):
    x1, y1, x2, y2 = text_block['x_1'], text_block['y_1'], text_block['x_2'], text_block['y_2']
    # Load the image
    img = cv2.imread(imagepath)
    # Add 10 pixels to each side of the rectangle
    x1 -= 5
    y1 -= 5
    x2 += 5
    y2 += 5
    
    # Ensure the coordinates are within the image boundaries
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(img.shape[1], x2)
    y2 = min(img.shape[0], y2)
    
    # Crop the specified region
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    
    # Save the cropped image
    cropped_image_path = "text_block.png"
    cv2.imwrite(cropped_image_path, cropped_image)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

#extract and return text from title block
@timeit
def process_title(title_block,imagepath, output):
    x1, y1, x2, y2 = title_block['x_1'], title_block['y_1'], title_block['x_2'], title_block['y_2']
    # Load the image
    img = cv2.imread(imagepath)
    # Add 10 pixels to each side of the rectangle
    x1 -= 5
    y1 -= 5
    x2 += 5
    y2 += 5
    
    # Ensure the coordinates are within the image boundaries
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(img.shape[1], x2)
    y2 = min(img.shape[0], y2)
    
    # Crop the specified region
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    
    # Save the cropped image
    cropped_image_path = "title_block.png"
    cv2.imwrite(cropped_image_path, cropped_image)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

#extract and return text from list block
@timeit
def process_list(list_block,imagepath, output):
    x1, y1, x2, y2 = list_block['x_1'], list_block['y_1'], list_block['x_2'], list_block['y_2']
    # Load the image
    img = cv2.imread(imagepath)
    # Add 10 pixels to each side of the rectangle
    x1 -= 5
    y1 -= 5
    x2 += 5
    y2 += 5
    
    # Ensure the coordinates are within the image boundaries
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(img.shape[1], x2)
    y2 = min(img.shape[0], y2)
    
    # Crop the specified region
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    
    # Save the cropped image
    cropped_image_path = "list_block.png"
    cv2.imwrite(cropped_image_path, cropped_image)
    #extraction of text from cropped image using pytesseract
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    #delete cropped image
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

#upload figure to aws and return aws url
@timeit
def upload_to_aws_s3(figure_image_path, figureId): 
    folderName=os.environ['AWS_IMAGE_UPLOAD_FOLDER']
    s3_key = f"{folderName}/{figureId}.png"
    # Upload the image to the specified S3 bucket
    s3.upload_file(figure_image_path, bucket_name, s3_key)
    # Get the URL of the uploaded image
    figure_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"

    return figure_url 

@timeit
def extract_text_equation_with_nougat(image_path, page_equations, page_num, bookname, bookId):
    pdf_path ="page.pdf"
    with open(pdf_path, "wb") as pdf_file, open(image_path, "rb") as image_file:
        pdf_file.write(img2pdf.convert(image_file))
    latex_text=get_latext_text(pdf_path,page_num, bookname, bookId)
    latex_text = latex_text.replace("[MISSING_PAGE_EMPTY:1]", "")
    if latex_text == "":
        latex_text = ""
    pattern = r'(\\\(.*?\\\)|\\\[.*?\\\])'
    
    def replace_with_uuid(match):
        equationId = uuid.uuid4().hex
        match_text = match.group()
        text_to_speech=latext_to_text_to_speech(match_text)
        page_equations.append({'id': equationId, 'text': match_text, 'text_to_speech':text_to_speech})
        return f'{{{{equation:{equationId}}}}}'
    
    page_content = re.sub(pattern, replace_with_uuid, latex_text)
    page_content = re.sub(r'\s+', ' ', page_content).strip()
    if os.path.exists(pdf_path):
        os.remove(pdf_path)
    return page_content

@timeit
def process_equation(equation_block, imagepath, output, page_equations):
    x1, y1, x2, y2 = equation_block['x_1'], equation_block['y_1'], equation_block['x_2'], equation_block['y_2']
    # # Load the image
    img = cv2.imread(imagepath)

    x1 -= 5
    y1 -= 5
    x2 += 5
    y2 += 5
    
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(img.shape[1], x2)
    y2 = min(img.shape[0], y2)
    
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    equation_image_path ="cropped_equation.png"
    cv2.imwrite(equation_image_path, cropped_image)

    equationId=uuid.uuid4().hex
    output += f"{{{{equation:{equationId}}}}}"

    img = Image.open(equation_image_path)
    model = LatexOCR()
    latex_text= model(img)

    text_to_speech=latext_to_text_to_speech(latex_text)

    page_equations.append(
       {'id': equationId, 'text':latex_text, 'text_to_speech':text_to_speech} 
    )

    if os.path.exists(equation_image_path):
        os.remove(equation_image_path)
    return output
   
@timeit
def get_latext_text(pdf_path, page_num, bookname, bookId):
    try:
        command=[
            "nougat",
            pdf_path,
            "--no-skipping"
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except Exception as e:
        print(f"An error occurred while processing {bookname}, page {page_num} with nougat: {str(e)}")
        error={"error":str(e),"page_number":page_num, "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno}
        document=error_collection.find_one({"bookId":bookId})
        if document:
            error_collection.update_one({"_id": document["_id"]}, {"$push": {"pages": error}})
        else:
            new_error_doc = {"bookId": bookId, "book": bookname, "error_pages": [error]}
            error_collection.insert_one(new_error_doc)

@timeit
def latext_to_text_to_speech(text):
    # Remove leading backslashes and add dollar signs at the beginning and end of the text
    text = "${}$".format(text.lstrip('\\'))
    # Convert the LaTeX text to text to speech
    text_to_speech = latex_to_text(text)
    return text_to_speech

@timeit
def get_figure_and_captions(book_path,bookname,bookId):
    document = figure_caption.find_one({"bookId":bookId})
    if document:
        return
    output_directory = os.path.abspath("pdffiles")
    book_output = os.path.abspath('output')
    os.makedirs(output_directory, exist_ok=True)
    os.makedirs(book_output, exist_ok=True)
    with open(book_path, 'rb') as pdf_file:
        pdf_reader =PyPDF2.PdfReader(pdf_file)
        num_pages = len(pdf_reader.pages)
        pages_per_split = 15
        for i in range(0, num_pages, pages_per_split):
            pdf_writer = PyPDF2.PdfWriter()
            for page_num in range(i, min(i + pages_per_split, num_pages)):
                 page = pdf_reader.pages[page_num]
                 pdf_writer.add_page(page)

            # Save the smaller PDF to the output directory
            output_filename = os.path.join(output_directory, f'output_{i // pages_per_split + 1}.pdf')
            with open(output_filename, 'wb') as output_file:
                pdf_writer.write(output_file)   
    try:
        book_data=extract_figure_and_caption(output_directory, book_output)
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)   
        if os.path.exists(book_output):
            shutil.rmtree(book_output)
        if book_data:
            figure_caption.insert_one({"bookId": bookId, "book": bookname, "pages": book_data})
            print("Book's figure and figure caption saved in the database")
        else:
            print(f"no figure detected by pdfigcapx for this book {bookname}")
    except Exception as e:
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)   
        if os.path.exists(book_output):
            shutil.rmtree(book_output)
        print(f"Unable to get figure and figure caption for this {bookname}, {str(e)}, line_number {traceback.extract_tb(e.__traceback__)[-1].lineno}")
        return []



books = get_all_books_names(bucket_name, folder_name+'/')

for idx, book in enumerate(books):
    start_book=0
    start_page=0
    bookId=None
    prog_doc=list(book_progress.find())
    book_com=list(book_number.find())
    if len(prog_doc)>0:
        start_page=prog_doc[-1]['page_num']
        start_book=prog_doc[-1]['book_number']-1
        bookId=prog_doc[-1]['bookId']
    if len(book_com)>0:
        start_book=book_com[0]['book_number']
    if(idx<start_book):
        print('skipping this book', book)
        continue
    if book.endswith('.pdf'):
        current_book_number=idx+1
        process_book(book, start_page, bookId)
    else:
        print(f"skipping this {book} as it it is not a pdf file")
        continue