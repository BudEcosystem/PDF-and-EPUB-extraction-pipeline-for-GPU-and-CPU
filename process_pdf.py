import layoutparser as lp
import pytesseract
from PIL import Image
import os
import fitz
import boto3
import cv2
import pymongo
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

# Create an S3 client
s3 = boto3.client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   region_name=aws_region)

bucket_name = 'bud-datalake'
folder_name = 'book-set-2'

# //returns list of booknames
def get_all_books_names(bucket_name, folder_name):
  contents = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
  pdf_file_names = [obj['Key'] for obj in contents.get('Contents', [])]
  book_names = [file_name.split('/')[1] for file_name in pdf_file_names]
  return book_names

# downlads particular book from aws and save it to system and return the bookpath
def download_book_from_aws(bookname):
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
        return None

def process_book(bookname):
    book_folder = bookname.replace('.pdf', '')
    book_path = download_book_from_aws(bookname)
    
    if not book_path:
        print(f"Error: Could not download {bookname} from AWS.")
        return  # Exit the function if book_path is not available
    
    os.makedirs(book_folder, exist_ok=True)
    book = PdfReader(book_path)  # Use book_path instead of bookname
    print(bookname)
    num_pages = len(book.pages)
    with Pool() as pool:
        page_numbers = range(num_pages)
        #use parrallel proccesing to process pages concurrently
        page_data = pool.starmap(process_page,[(page_num, book_path,book_folder) for page_num in page_numbers])

    bookdata_doc = {
        "book": bookname,
        "pages": page_data
    }
    bookdata.insert_one(bookdata_doc)
    
def process_page(page_num, book_path, book_folder):
    pages_data=[]
    print(page_num, "done")
    pdf_images = fitz.open(book_path)
    page_image = pdf_images[page_num]
    book_image = page_image.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
    image_path = os.path.join(book_folder, f'page_{page_num + 1}.jpg')
    book_image.save(image_path)
    page_data,tables_array = process_image(image_path)
    pageId= str(uuid.uuid4())
    page_obj={
        "id":pageId,
        "content":page_data,
        "tables":tables_array 
    }
    pages_data.append(page_obj)
    return pages_data

def process_image(imagepath):
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
        tables_array=[]
        #sort blocks based on their region
        page_data = sort_text_blocks_and_extract_data(final_layout,imagepath, tables_array)
        print(page_data)
        #process blocks for extracting different information(text,table,figure etc.)    
        return page_data, tables_array
    else:
        print("try that image with different model")

def sort_text_blocks_and_extract_data(blocks, imagepath,tables_array):
    sorted_blocks = sorted(blocks, key=lambda block: (block.block.y_1 + block.block.y_2) / 2)
    output=""
    for block in sorted_blocks:
        if block.type == "Table":
            output = process_table(block, imagepath,output, tables_array)
        elif block.type == "Figure":
            output = process_figure(block, imagepath, output)
        elif block.type == "Text":
            output = process_text(block,imagepath, output)
        elif block.type == "Title":
            output = process_title(block,imagepath, output)
        elif block.type == "List":
            output = process_list(block,imagepath, output)
    return output

def process_table(table_block, imagepath, output,tables_array):
    x1, y1, x2, y2 = table_block.block.x_1, table_block.block.y_1, table_block.block.x_2, table_block.block.y_2
    # Load the image
    img = cv2.imread(imagepath)
    # Add 10 pixels to each side of the rectangle
    x1 -= 50
    y1 -= 50
    x2 += 50
    y2 += 50
    
    # Ensure the coordinates are within the image boundaries
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(img.shape[1], x2)
    y2 = min(img.shape[0], y2)
    # Crop the specified region
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    
    # Save the cropped image
    cropped_image_path = "cropped_table.png"

    cv2.imwrite(cropped_image_path, cropped_image)
    #process table: 
    tables=process_book_page(cropped_image_path)
    tables_array.append(tables)
    # Return the path to the cropped image
    tableId =uuid.uuid4().hex
    output+=f"{{{{table:{tableId}}}}}"
    return output

def process_figure(figure_block, imagepath, output):
    x1, y1, x2, y2 = figure_block.block.x_1, figure_block.block.y_1, figure_block.block.x_2, figure_block.block.y_2
    # Load the image
    img = cv2.imread(imagepath)
    # Add 10 pixels to each side of the rectangle
    x1 -= 50
    y1 -= 50
    x2 += 50
    y2 += 50
    
    # Ensure the coordinates are within the image boundaries
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(img.shape[1], x2)
    y2 = min(img.shape[0], y2)
    
    # Crop the specified region
    cropped_image = img[int(y1):int(y2), int(x1):int(x2)]
    
    # Save the cropped image
    cropped_image_path = "cropped_figure.png"
    cv2.imwrite(cropped_image_path, cropped_image)
    
    print(cropped_image_path)
    figureId =uuid.uuid4().hex
    output+=f"{{{{figure:{figureId}}}}}"
    return output

def process_text(text_block,imagepath, output):
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
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text

    return output

def process_title(title_block,imagepath, output):
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
    cropped_image_path = "text_block.png"
    cv2.imwrite(cropped_image_path, cropped_image)
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text

    # titleId =uuid.uuid4().hex
    # output+=f"{{{{title:{titleId}}}}}"
    return output

def process_list(list_block,imagepath, output):
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
    image =Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output+=text
    # listId =uuid.uuid4().hex
    # output+=f"{{{{list:{listId}}}}}"
    return output


if __name__=="__main__":
    #call process_book 
    #write any bookname which available in aws book set
    process_book("page_28 (2).pdf")