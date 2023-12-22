"""
main code page
structure (xpdf_process):
1. Read pdfs from input folder
2. Figure and caption pair detection
    2.1. graphical content detection
    2.2 page segmentation
    2.3 figure detetion
    2.4 caption association

3. Mess up pdf processing


Writen by Pengyuan Li

Start from 19/10/2017
1.0 version 28/02/2018

"""

import os
from xpdf_process import figures_captions_list
import subprocess
from utils import get_page_num_from_split_path
from dotenv import load_dotenv

load_dotenv()

pdf_batch_size = int(os.environ["PDF_BATCH_SIZE"])


def change_figures_dict(figures, from_page, to_page):
    """
    Changes the page numbers in the figures dictionary.
    """
    new_figures = {}
    page_split_size = pdf_batch_size
    factor = (from_page - 1) // page_split_size
    if factor == 0:
        return figures
    for i in range(from_page, to_page + 1):
        page_number = f"page{i}.png"
        new_figures[page_number] = figures[f"page{i - page_split_size*factor}.png"]
    return new_figures

def create_dict_with_page_numbers(from_page, to_page):
    """
    Creates a dictionary with page numbers as keys and figures as values.
    """
    figures = {}
    for i in range(from_page, to_page + 1):
        page_number = f"page{i}.png"
        figures[page_number] = []
    return figures

def extract_figure_and_caption(input_path, output_path):
    """
    Extracts figures and captions from pdfs in the input path and saves them in the output path.
    """
    xpdf_path = output_path +'/xpdf/'
    if not os.path.isdir(xpdf_path):
        os.mkdir(xpdf_path)
    # Read each files in the input path
    all_data=[]
    # ["book-set-2/123/splits/123_0_1-30.pdf"]
    pdf_files = [pdf for pdf in os.listdir(input_path) if pdf.endswith('.pdf') and not pdf.startswith('._')]
    print("pdf_files process by figcap >> ", pdf_files)
    for pdf in pdf_files:
        _, _, from_page, to_page = get_page_num_from_split_path(pdf)
        print(f"from_page: {from_page}, to_page: {to_page}")
        try:
            if not os.path.isdir(xpdf_path + pdf[:-4]):
                _ = subprocess.check_output([
                    os.environ['Xpdf_PATH'],
                    input_path + '/' + pdf,
                    xpdf_path + pdf[:-4] + '/'
                ])
            figures, _ = figures_captions_list(input_path, pdf, xpdf_path)
            all_data.append(change_figures_dict(figures, from_page, to_page))
        except Exception as e:
            print("Error in figure caption extraction: ", e)
            all_data.append(create_dict_with_page_numbers(from_page, to_page))

    book_data=[]
    for obj in all_data:
        for figure in obj:
            page_no = int(figure[:-4][4:])
            bboxes=obj[figure]

            for bbox in bboxes:
                
                if len(bbox[1])>0:
                    data = {
                        'page_num': page_no,
                        'figure_bbox': bbox[0],
                        'type': 'Figure',
                        'caption_text': bbox[1][1]
                    }
                    book_data.append(data)
                else:
                    data = {
                        'page_num': page_no,
                        'figure_bbox': bbox[0],
                        'type':'Figure',
                        'caption_text':''
                    }
                    book_data.append(data)

    # why ?
    # figure_extracted=True
    # for item in book_data:
    #     if not 'page_num' in item:
    #         figure_extracted=False

    # # why ?
    # if not figure_extracted:
    #     for item in book_data:
    #         print("item >>> ", type(item))
    #         for _, book_data in item.items():
    #             print("bookData >>> ", book_data)
    #             if 'pages_annotated' in book_data:
    #                 book_data=[]

    return book_data

if __name__=='__main__':
    input_files_path = "/home/developer/prakash/book-set-2/123/splits"
    output_files_directory = "/home/developer/prakash/book-set-2/123/figure_output"
    extract_figure_and_caption(input_files_path, output_files_directory)

    