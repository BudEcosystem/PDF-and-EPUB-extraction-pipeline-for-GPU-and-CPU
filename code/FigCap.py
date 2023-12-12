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
import os
from dotenv import load_dotenv

load_dotenv()


def change_figures_dict(figures, from_page, to_page):
    """
    Changes the page numbers in the figures dictionary.
    """
    new_figures = {}
    page_split_size = 15
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
    pdf_files = [pdf for pdf in os.listdir(input_path) if pdf.endswith('.pdf') and not pdf.startswith('._')]
    # pdf_files.sort(key=lambda x: int(x.split("_")[1].split(".")[0]))
    pdf_files.sort(key=lambda x: int(x.split("_")[2].split(".")[0]))
    print("pdf_files process by figcap >> ", pdf_files)
    for pdf in pdf_files:
        print(pdf)
        from_page = int(pdf.split("_")[1].split("-")[0])
        to_page = int(pdf.split("_")[1].split("-")[1])
        print(f"from_page: {from_page}, to_page: {to_page}")
        # data = {}
        # images = renderer.render_pdf((input_path + '/' + pdf), 370)
        # data[pdf] = {}
        # data[pdf]['figures'] = []
        # data[pdf]['pages_annotated'] = []
        # pdf_flag = 0
        try:
            if not os.path.isdir(xpdf_path+pdf[:-4]):
                _ = subprocess.check_output([os.environ['Xpdf_PATH'], input_path+'/'+pdf, xpdf_path+pdf[:-4]+'/'])
        # except Exception as e:
        #     print(e)
        #     pdf_flag = 1
        #     all_data.append(create_dict_with_page_numbers(from_page, to_page))
        # if pdf_flag == 0:
        #     try:
            figures, _ = figures_captions_list(input_path, pdf, xpdf_path)
            all_data.append(change_figures_dict(figures, from_page, to_page))
        except Exception as e:
            print("Error in figure caption extraction: ", e)
            all_data.append(create_dict_with_page_numbers(from_page, to_page))
            # flag = 0
            # wrong_count = 0
            # while flag==0 and wrong_count < 5:
            #     info = {}
            #     try:
            #         figures, info = figures_captions_list(input_path, pdf, xpdf_path)
            #         flag = 1

            #     except Exception as e:
            #         print("Error in figure caption extraction: ", e)
            #         wrong_count = wrong_count + 1
            #         # sonali : why sleep 5 seconds
            #         # time.sleep(5)
            #         info['fig_no_est'] = 0
            #         figures = []

    # add increamental page numbers to subsequent split pdf outputs
    # for i in range(1, len(all_data)):
    #     if all_data[i-1]:
    #         last_key = list(all_data[i - 1].keys())[-1]
    #         page_number = int(last_key.split('.')[0][4:]) + 1
    #         if all_data[i]:
    #             for key in all_data[i]:
    #                 new_key = f"page{page_number}.png"
    #                 all_data[i][new_key] = all_data[i].pop(key)
    #                 page_number += 1


    book_data=[]
    for obj in all_data:
        for figure in obj:
            page_no = int(figure[:-4][4:])
            bboxes=obj[figure]

            for bbox in bboxes:
                
                if len(bbox[1])>0:
                    data = {
                        'page_num':page_no,
                        'figure_bbox':bbox[0],
                        'type':'Figure',
                        'caption_text':bbox[1][1]
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
    input_files_path = "/home/azureuser/prakash/pdf_extraction_pipeline/book-set-2/456/splits"
    output_files_directory = "/home/azureuser/prakash/pdf_extraction_pipeline/book-set-2/456/output"
    extract_figure_and_caption(input_files_path, output_files_directory)

    