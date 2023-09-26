import layoutparser as lp
import pytesseract
from PIL import Image
import os
import fitz
import shutil
import boto3
import re
import cv2
# import urlpars
import pymongo
from urllib.parse import urlparse
import urllib
import time
import uuid
from PyPDF2 import PdfReader
from multiprocessing import Pool
from tablecaption import process_book_page

# Configure AWS credentials
aws_access_key_id = 'AKIA4CKBAUILYLX23AO7'
aws_secret_access_key = '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'
aws_region = 'ap-south-1'

client = pymongo.MongoClient("mongodb+srv://prakash:prak1234@cluster0.nbtkiwp.mongodb.net")
db = client.aws_book_set_2
bookdata = db.bookdata
error_collection = db.error_collection

# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

# bucket_name = 'bud-datalake'
# folder_name = 'book-set-2'

# returns list of booknames
def get_all_books_names(bucket_name, folder_name):
  contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
  pdf_file_names = [obj['Key'] for obj in contents.get('Contents', [])]
  book_names = [file_name.split('/')[1] for file_name in pdf_file_names]
  return book_names

# downlads particular book from aws and save it to system and return the bookpath
def download_book_from_aws(url):
    try:
        start_time = time.time()
        parsed_url = urlparse(url)
        bucket_name = parsed_url.path.split('/')[-1]
        file_key_list = list(filter(lambda x: x.startswith('prefix='), parsed_url.query.split('&')))
        file_key = file_key_list[0].split('=')[1]
        print(file_key)
        folder_name = file_key_list[0].split('/')[0]
        folder_name = folder_name.replace('prefix=', '') 
        bookname = file_key_list[0].split('/')[-1]
        os.makedirs(folder_name, exist_ok=True)
        local_path = os.path.join(folder_name, os.path.basename(file_key))
        s3.download_file(bucket_name, file_key, local_path)
        end_time=time.time()
        total_time = end_time - start_time
        print(f"download_book_from_aws took {total_time} seconds")
        return local_path,bookname 
    except Exception as e:
        print("An error occurred:", e)
        return None
 
def process_book(url):
    start_time=time.time()
    book_path,bookname = download_book_from_aws(url)
    book_folder = book_path.split('/')[-1].replace('.pdf','')
    if not book_path:
         return 
    os.makedirs(book_folder, exist_ok=True)
    book = PdfReader(book_path)  # Use book_path instead of bookname
    print(bookname)
    num_pages = len(book.pages)
    print(f"{bookname} has total {num_pages} page")
    num_cpu_cores = os.cpu_count()
    try:
        with Pool(processes=num_cpu_cores) as pool:
            page_numbers = range(num_pages)
        #use parrallel proccesing to process pages concurrently
            page_data = pool.starmap(process_page,[(page_num, book_path,book_folder, bookname) for page_num in page_numbers])
    
        bookdata_doc = {
            "book": bookname,
            "pages": page_data
        }
        bookdata.insert_one(bookdata_doc)
        end_time=time.time()
        total_time=end_time-start_time
        print(f"process_book took {elapsed_time} seconds")
    except Exception as e:
        error_collection.update_one({"book": book}, {"$set": {"error": str(e)}}, upsert=True)
    #find document by name replace figure caption with ""
    document = bookdata.find_one({"book":bookname})
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
def process_page(page_num, book_path, book_folder, bookname):
    start_time=time.time()
    pages_data=[]
    pdf_images = fitz.open(book_path)
    page_image = pdf_images[page_num]
    book_image = page_image.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
    image_path = os.path.join(book_folder, f'page_{page_num + 1}.jpg')
    book_image.save(image_path)
    page_content,page_tables,page_figures = process_image(image_path, page_num, bookname)
    pageId= uuid.uuid4().hex
    page_obj={
        "id":pageId,
        "text":page_content,
        "tables":page_tables,
        "figures":page_figures
    }
    os.remove(image_path)
    end_time=time.time()
    total_time=end_time-start_time
    print(f"process_page took {total_time} seconds")
    return page_obj

#detect layout and return page data
def process_image(imagepath, page_num, bookname):
    try:
        start_time=time.time()
        image = cv2.imread(imagepath)
        image = image[..., ::-1]

        model = lp.Detectron2LayoutModel('lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config',
                                 extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                                 label_map={0: "Text", 1: "Title", 2: "List", 3:"Table", 4:"Figure"})

        layout1 = model.detect(image)
    
        #detect extract table layout using TableBank model
        model2 = lp.Detectron2LayoutModel('lp://TableBank/faster_rcnn_R_101_FPN_3x/config',
                                     extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                                     label_map={0:"Table"})
    
        layout2=model2.detect(image)
    
        final_layout = []
        for block in layout1:
            if block.type != "Table":
                final_layout.append(block)

        # Add "Table" blocks from layout2 to the new list
        for block in layout2:
            if block.type == "Table":
                final_layout.append(block)
    
        if final_layout:
            page_tables=[]
            page_figures=[]
            #sort blocks based on their region
            page_content = sort_text_blocks_and_extract_data(final_layout,imagepath,page_tables,page_figures)
            #process blocks for extracting different information(text,table,figure etc.)    
            return page_content,page_tables,page_figures
        else:
            print(f"Could not detect layout for page number {page_num} of book {bookname} Try a different model.")
            error = {"page_number": page_num, "error":"Could not detect layout for this page. Try a different model."}
            error_collection.update_one({"book": bookname}, {"$push": {"pages": error}}, upsert=True)
            return "", [], []
        end_time=time.time()
        total_time=end_time-start_time
        print(f"process_image took {total_time} seconds")
    except Exception as e:
        print(f"An error occurred while processing {bookname}, page {page_num}: {str(e)}")
        error = {"page_number": page_num, "error":str(e)}
        error_collection.update_one({"book": bookname}, {"$push": {"pages": error}}, upsert=True)
        return "", [], []

#sort the layout blocks and return page data 
def sort_text_blocks_and_extract_data(blocks, imagepath,page_tables, page_figures):
    start_time=time.time()
    sorted_blocks = sorted(blocks, key=lambda block: (block.block.y_1 + block.block.y_2) / 2)
    output = ""
    
    # Initialize variables to keep track of the previous and next blocks
    prev_block = None
    next_block = None
    
    for i, block in enumerate(sorted_blocks):
        if i > 0:
            prev_block = sorted_blocks[i - 1]
        if i < len(sorted_blocks) - 1:
            next_block = sorted_blocks[i + 1]   
        if block.type == "Table":
            output = process_table(block, imagepath, output, page_tables)
        elif block.type == "Figure":
            output = process_figure(block, imagepath, prev_block, next_block, output, page_figures)
        elif block.type == "Text":
            output = process_text(block, imagepath, output)
        elif block.type == "Title":
            output = process_title(block, imagepath, output)
        elif block.type == "List":
            output = process_list(block, imagepath, output)

    page_content = re.sub(r'\s+', ' ', output).strip()

    end_time=time.time()
    total_time=end_time-start_time
    print(f"sorting block and extracting took {total_time} seconds")
    return page_content

#extract table and table_caption and return table object {id, data, caption}
def process_table(table_block, imagepath, output, page_tables):
    start_time=time.time()
    x1, y1, x2, y2 = table_block.block.x_1, table_block.block.y_1, table_block.block.x_2, table_block.block.y_2
    # Load the image
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
    end_time=time.time()
    total_time=end_time-start_time
    print(f"process_table took {total_time} seconds")
    return output

#extract figure and figure_caption and return figure object {id, figureUrl, caption}
def process_figure(figure_block, imagepath, prev_block, next_block, output, page_figures):
    start_time=time.time()
    caption=""
    # Process the "Figure" block
    x1, y1, x2, y2 = figure_block.block.x_1, figure_block.block.y_1, figure_block.block.x_2, figure_block.block.y_2
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
    figure_image_path = f"figure_6.png"
    cv2.imwrite(figure_image_path,figure_bbox)
    figureId=uuid.uuid4().hex
    output += f"{{{{figure:{figureId}}}}}"

    if prev_block:
        prev_x1, prev_y1, prev_x2, prev_y2 = prev_block.block.x_1, prev_block.block.y_1, prev_block.block.x_2, prev_block.block.y_2
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
        next_x1, next_y1, next_x2, next_y2 = next_block.block.x_1, next_block.block.y_1, next_block.block.x_2, next_block.block.y_2
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

    end_time=time.time()
    total_time=end_time-start_time
    print(f"process_figure took {total_time} seconds")
    return output    

#extract and return text from text block
def process_text(text_block,imagepath, output):
    start_time=time.time()
    x1, y1, x2, y2 = text_block.block.x_1, text_block.block.y_1, text_block.block.x_2, text_block.block.y_2
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

    end_time=time.time()
    total_time=end_time-start_time
    print(f"process_text took {total_time} seconds")
    return output

#extract and return text from title block
def process_title(title_block,imagepath, output):
    start_time=time.time()
    x1, y1, x2, y2 = title_block.block.x_1, title_block.block.y_1, title_block.block.x_2, title_block.block.y_2
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

    end_time=time.time()
    total_time=end_time-start_time
    print(f"process_title took {total_time} seconds")
    return output

#extract and return text from list block
def process_list(list_block,imagepath, output):
    start_time=time.time()
    x1, y1, x2, y2 = list_block.block.x_1, list_block.block.y_1, list_block.block.x_2, list_block.block.y_2
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

    end_time=time.time()
    total_time=end_time-start_time
    print(f"process_list took {total_time} seconds")
    return output

#upload figure to aws and return aws url
def upload_to_aws_s3(figure_image_path, figureId): 
    start_time=time.time()
    folderName="book-set-2-Images"
    s3_key = f"{folderName}/{figureId}.png"
    # Upload the image to the specified S3 bucket
    s3.upload_file(figure_image_path, bucket_name, s3_key)
    # Get the URL of the uploaded image
    figure_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"

    end_time=time.time()
    total_time=end_time-start_time
    print(f"uppload to aws s3 took {total_time} seconds")
    return figure_url 
    
if __name__=="__main__":
    # process all books
    # books = get_all_books_names('bud-datalake', 'book-set-2/')
    # print(len(books))
    # for book in books:
    #     if book.endswith('.pdf'):
    #         process_book(book)
    #     else:
    #         print(f"skipping this {book} as it it is not pdf")
    #         error_collection.update_one({"book": book}, {"$set": {"error": f"{book} is not a pdf"}}, upsert=True)
    #         continue
    
    # process single book
    # process_book("A Beginner's Guide to R - Alain Zuur- Elena N Ieno- Erik Meesters.pdf")
      process_book("https://s3.console.aws.amazon.com/s3/object/bud-datalake?region=ap-southeast-1&prefix=bud-ebooks/00a85ef0-160d-432e-baa8-1d2f16277c2f.pdf")