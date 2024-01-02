import os
import regex as re
import cv2
import pytesseract
from PIL import Image
from pix2tex.cli import LatexOCR
from latext import latex_to_text
from utils import (
    generate_unique_id,
    generate_image_str,
    upload_to_aws_s3,
    crop_image,
    timeit,
)
from pdf_producer import send_to_queue

latex_model = LatexOCR()

# def get_fig_caption(block, image_path):
#     caption = ""
#     image_id = generate_unique_id()
#     block_image_path = crop_image(block, image_path, image_id)
#     # extraction of text from cropped image using pytesseract
#     image = Image.open(block_image_path)
#     text = pytesseract.image_to_string(image)
#     text = re.sub(r'\s+', ' ', text).strip()
#     pattern = r"(Fig\.|Figure)\s+\d+"
#     match = re.search(pattern, text)
#     if match:
#         caption = text
#     if os.path.exists(block_image_path):
#         os.remove(block_image_path)
# return caption/

@timeit
def process_table(table_block, image_path, bookId, page_num):
    output = None
    x1, y1, x2, y2 = (
        table_block["x_1"],
        table_block["y_1"],
        table_block["x_2"],
        table_block["y_2"],
    )
    img = cv2.imread(image_path)
    y1 -= 70
    if y1 < 0:
        y1 = 0
    x1 = 0
    x2 += 20
    if x2 > img.shape[1]:
        x2 = img.shape[1]
    y2 += 20
    if y2 > img.shape[0]:
        y2 = img.shape[0]
    cropped_image = img[int(y1) : int(y2), int(x1) : int(x2)]
    tableId = generate_unique_id()
    table_image_path = os.path.abspath(f"cropped_{tableId}.png")
    cv2.imwrite(table_image_path, cropped_image)
    output = f"{{{{table:{tableId}}}}}"
    data = {"img": generate_image_str(bookId, table_image_path, save=False)}
    bud_table_msg = {
        "tableId": tableId,
        "data": data,
        "page_num": page_num,
        "bookId": bookId,
    }
    send_to_queue("bud_table_extraction_queue", bud_table_msg)
    if os.path.exists(table_image_path):
        os.remove(table_image_path)
    return output


# def process_figure(figure_block, image_path):
#     output = None
#     figureId = generate_unique_id()
#     figure_image_path = crop_image(figure_block, image_path, figureId)
#     output = f"{{{{figure:{figureId}}}}}"

#     figure_url = upload_to_aws_s3(figure_image_path, figureId)
#     figure = {
#         "id": figureId,
#         "url": figure_url,
#         "caption": figure_block['caption']
#     }
#     if os.path.exists(figure_image_path):
#         os.remove(figure_image_path)
#     return output, figure

@timeit
def process_publaynet_figure(figure_block, image_path):
    print("publaynet figure")
    output = None
    caption = ""
    figureId = generate_unique_id()
    figure_image_path = crop_image(figure_block, image_path, figureId)
    output = f"{{{{figure:{figureId}}}}}"

    figure_url = upload_to_aws_s3(figure_image_path, figureId)
    figure = {"id": figureId, "url": figure_url, "caption": caption}
    if os.path.exists(figure_image_path):
        os.remove(figure_image_path)
    return output, figure

@timeit
def process_text(text_block, image_path):
    textId = generate_unique_id()
    cropped_image_path = crop_image(text_block, image_path, textId)
    image_data = Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image_data)
    output = text
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_title(title_block, image_path):
    title_id = generate_unique_id()
    cropped_image_path = crop_image(title_block, image_path, title_id)
    # extraction of text from cropped image using pytesseract
    image_data = Image.open(cropped_image_path)
    output = pytesseract.image_to_string(image_data)
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_list(list_block, image_path):
    list_id = generate_unique_id()
    cropped_image_path = crop_image(list_block, image_path, list_id)
    image = Image.open(cropped_image_path)
    text = pytesseract.image_to_string(image)
    output = text
    if os.path.exists(cropped_image_path):
        os.remove(cropped_image_path)
    return output

@timeit
def process_equation(equation_block, image_path):
    equation_id = generate_unique_id()
    equation_image_path = crop_image(equation_block, image_path, equation_id)
    output = f"{{{{equation:{equation_id}}}}}"
    img = Image.open(equation_image_path)
    latex_text = ""
    try:
        latex_text = latex_model(img)
    except Exception as e:
        print("error while extracting equation using latex ocr", e)
    text_to_speech = latext_to_text_to_speech(latex_text)
    equation = {"id": equation_id, "text": latex_text, "text_to_speech": text_to_speech}
    if os.path.exists(equation_image_path):
        os.remove(equation_image_path)
    return output, equation

@timeit
def latext_to_text_to_speech(text):
    text = "${}$".format(text.lstrip("\\"))
    text_to_speech = latex_to_text(text)
    return text_to_speech
