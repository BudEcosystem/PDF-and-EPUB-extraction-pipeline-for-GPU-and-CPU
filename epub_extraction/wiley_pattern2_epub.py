import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import os
from PIL import Image
from pix2tex.cli import LatexOCR
from bs4 import BeautifulSoup, NavigableString
from utils import (
    timeit,
    mongo_init,
    parse_table,
    get_all_books_names,
    get_s3,
    get_file_object_aws,
    get_toc_from_ncx,
    get_toc_from_xhtml,
    generate_unique_id,
    latext_to_text_to_speech,
)

latex_ocr = LatexOCR()

#change folder and bucket name as required.
bucket_name = "bud-datalake"
# folder_name = "Books/Oct29-1/"
folder_name='Books/Oct29-1/'
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init("epub_testing")
oct_toc = db.oct_toc
oct_no_toc = db.oct_no_toc
oct_chapters = db.oct_chapters
files_with_error = db.files_with_error
extracted_books = db.extracted_books
publisher_collection = db.publishers


def download_aws_image(key, book):
    try:
        book_folder = os.path.join(folder_name, book)
        os.makedirs(book_folder, exist_ok=True)
        local_path = os.path.join(book_folder, os.path.basename(key))
        s3 = get_s3()
        s3.download_file(bucket_name, key, local_path)
        return os.path.abspath(local_path)
    except Exception as e:
        print(e)
        return None


def download_epub_from_s3(bookname, s3_key):
    try:
        local_path = os.path.abspath(os.path.join(folder_name, f"{bookname}.epub"))
        os.makedirs(folder_name, exist_ok=True)
        s3 = get_s3()
        s3.download_file(bucket_name, s3_key, local_path)
        return local_path
    except Exception as e:
        print(e)
        return None


@timeit
def parse_html_to_json(html_content, book, filename):
    # html_content = get_file_object_aws(book, filename)
    soup = BeautifulSoup(html_content, "html.parser")
    # h_tag = get_heading_tags(soup, h_tag=[])
    section_data = extract_data(soup.find("body"), book, filename, section_data=[])
    return section_data


def extract_data(elem, book, filename, section_data=[]):
    for child in elem.children:
        temp = {}
        if isinstance(child, NavigableString):
            if child.strip():
                if section_data:
                    section_data[-1]["content"] += child + " "
                else:
                    temp["title"] = ""
                    temp["content"] = child + " "
                    temp["figures"] = []
                    temp["tables"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

        elif child.name:
            if child.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                parent_figure = child.find_parent("figure")
                if not parent_figure:
                    if section_data and section_data[-1]["content"].endswith(
                        "{{title}} "
                    ):
                        section_data[-1]["content"] += child.text.strip() + " "
                    else:
                        temp["title"] = child.text.strip()
                        temp["content"] = "{{title}}" + " "
                        temp["figures"] = []
                        temp["tables"] = []
                        temp["code_snippet"] = []
                        temp["equations"] = []

            elif child.name == "img":
                print("figure here from img")
                img = {}
                img["id"] = generate_unique_id()
                aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                img["url"] = aws_path + child["src"]

                parent = child.find_parent("div")
                if parent:
                    figparent = parent.find_parent('div', class_="figure-contents")
                    if figparent:
                        divparent= figparent.find_parent("div", class_="figure")
                        if divparent:
                            figcaption=divparent.find('p', class_="title")
                            if figcaption:
                                img['caption']=figcaption.get_text(strip=True)
                    else:
                        figcaption =parent.find('p', class_="figurecaption")
                        if figcaption:
                            img['caption']=figcaption.get_text(strip=True)
                            # print("this is figure caption", img['caption'])


                

                if section_data:
                    section_data[-1]["content"] += "{{figure:" + img["id"] + "}} "
                    if "figures" in section_data[-1]:
                        section_data[-1]["figures"].append(img)
                    else:
                        section_data[-1]["figures"] = [img]

                else:
                    temp["title"] = ""
                    temp["content"] = "{{figure:" + img["id"] + "}} "
                    temp["figures"] = [img]
                    temp["tables"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

            elif child.name == "table":
                print("table here")
                caption_text = ""
                parent = child.find_parent("div", class_="table-contents")
                if parent:
                    tableparent = parent.find_parent("div",class_="table")
                    if tableparent:
                        tabcap = tableparent.find("p","title")
                        if tabcap:
                            caption_text = tabcap.get_text(strip=True)
                            print("this is table caption",caption_text)
                else:
                    previous_sibling=child.find_previous_sibling()
                    if previous_sibling:
                        tab_prev_sib_class=previous_sibling.get('class',[''])[0]
                        if tab_prev_sib_class=='tablecaption':
                            caption_text=previous_sibling.get_text(strip=True)
                            print("this is table caption", caption_text)


                table_id = generate_unique_id()
                table_data = parse_table(child)
                table = {"id": table_id, "data": table_data, "caption": caption_text}
                if section_data:
                    section_data[-1]["content"] += "{{table:" + table["id"] + "}} "
                    if "tables" in section_data[-1]:
                        section_data[-1]["tables"].append(table)
                        # section_data[-1]['tables'] = [table]
                    else:
                        section_data[-1]["tables"] = [table]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{table:" + table["id"] + "}} "
                    temp["tables"] = [table]
                    temp["figures"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

            elif child.name == "p" and (
                "equationnumbered" in child.get("class", [])
                or "equation" in child.get("class", [])
            ):
                equation_image = child.find("img")
                equation_Id = generate_unique_id()
                if equation_image:
                    aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                    img_url = aws_path + equation_image["src"]
                    img_key = img_url.replace(s3_base_url + "/", "")
                    equation_image_path = download_aws_image(img_key, book)
                    if not equation_image_path:
                        continue
                    try:
                        img = Image.open(equation_image_path)
                    except Exception as e:
                        print("from image equation", e)
                        continue
                    try:
                        latex_text = latex_ocr(img)
                    except Exception as e:
                        print("error while extracting latex code from image", e)
                        continue
                    text_to_speech = latext_to_text_to_speech(latex_text)
                    eqaution_data = {
                        "id": equation_Id,
                        "text": latex_text,
                        "text_to_speech": text_to_speech,
                    }
                    print(equation_image_path)
                    print("this is equation image from equation class")
                    os.remove(equation_image_path)
                else:
                    continue
                if section_data:
                    section_data[-1]["content"] += "{{equation:" + equation_Id + "}} "
                    if "equations" in section_data[-1]:
                        section_data[-1]["equations"].append(eqaution_data)
                    else:
                        section_data[-1]["equations"] = [eqaution_data]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{equation:" + equation_Id + "}} "
                    temp["tables"] = []
                    temp["figures"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = [eqaution_data]
            

            elif child.name == "div" and ("equation-contents" in child.get("class", [])):
                divele=child.find('div')
                if divele:
                    equation_image = divele.find("img")
                    equation_Id = generate_unique_id()
                    if equation_image:
                        aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                        img_url = aws_path + equation_image["src"]
                        img_key = img_url.replace(s3_base_url + "/", "")
                        equation_image_path = download_aws_image(img_key, book)
                        if not equation_image_path:
                            continue
                        try:
                            img = Image.open(equation_image_path)
                        except Exception as e:
                            print("from image equation", e)
                            continue
                        try:
                            latex_text = latex_ocr(img)
                        except Exception as e:
                            print("error while extracting latex code from image", e)
                            continue
                        text_to_speech = latext_to_text_to_speech(latex_text)
                        eqaution_data = {
                            "id": equation_Id,
                            "text": latex_text,
                            "text_to_speech": text_to_speech,
                        }
                        print(equation_image_path)
                        print("this is equation image from equation-contents class")
                        os.remove(equation_image_path)
                    else:
                        continue
                else:
                    continue
                if section_data:
                    section_data[-1]["content"] += "{{equation:" + equation_Id + "}} "
                    if "equations" in section_data[-1]:
                        section_data[-1]["equations"].append(eqaution_data)
                    else:
                        section_data[-1]["equations"] = [eqaution_data]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{equation:" + equation_Id + "}} "
                    temp["tables"] = []
                    temp["figures"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = [eqaution_data]

            # code oreilly publication
            elif child.name == "pre":
                print("code here")
                code_tags = child.find_all("code")
                code = ""
                if code_tags:
                    code = " ".join(
                        code_tag.get_text(strip=True) for code_tag in code_tags
                    )
                else:
                    code = child.get_text(strip=True)
                code_id = generate_unique_id()
                code_data = {"id": code_id, "code_snippet": code}
                if section_data:
                    section_data[-1]["content"] += "{{code_snippet:" + code_id + "}} "
                    if "code_snippet" in section_data[-1]:
                        section_data[-1]["code_snippet"].append(code_data)

                    else:
                        section_data[-1]["code_snippet"] = [code_data]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{code_snippet:" + code_id + "}} "
                    temp["tables"] = []
                    temp["figures"] = []
                    temp["code_snippet"] = [code_data]
                    temp["equations"] = []
            elif child.contents:
                section_data = extract_data(
                    child, book, filename, section_data=section_data
                )
        if temp:
            section_data.append(temp)
    return section_data


@timeit
def get_book_data(book):
    print("Book Name >>> ", book)
    toc = []
    # check if book exists in db toc collection
    db_toc = oct_toc.find_one({"book": book})
    if db_toc:
        toc = db_toc["toc"]
    if not toc:
        error = ""
        try:
            # get table of content
            toc_content = get_file_object_aws(book, "toc.ncx", folder_name, bucket_name)
            if toc_content:
                toc = get_toc_from_ncx(toc_content)
            else:
                toc_content = get_file_object_aws(
                    book, "toc.xhtml", folder_name, bucket_name
                )
                if toc_content:
                    toc = get_toc_from_xhtml(toc_content)
        except Exception as e:
            error = str(e)
            print(f"Error while parsing {book} toc >> {e}")
        if not toc:
            oct_no_toc.insert_one({"book": book, "error": error})
        else:
            oct_toc.insert_one({"book": book, "toc": toc})

    files = []
    order_counter = 0
    prev_filename = None

    for label, content in toc:
        content_split = content.split("#")
        if len(content_split) > 0:
            filename = content_split[0]
            if filename != prev_filename:
                if filename not in files:
                    file_in_error = files_with_error.find_one(
                        {"book": book, "filename": filename}
                    )
                    if file_in_error:
                        files_with_error.delete_one(
                            {"book": book, "filename": filename}
                        )
                    chapter_in_db = oct_chapters.find_one(
                        {"book": book, "filename": filename}
                    )
                    if chapter_in_db:
                        if chapter_in_db["sections"]:
                            continue
                        elif not chapter_in_db["sections"]:
                            oct_chapters.delete_one(
                                {"book": book, "filename": filename}
                            )

                    html_content = get_file_object_aws(
                        book, filename, folder_name, bucket_name
                    )
                    print(filename)
                    if html_content:
                        try:
                            json_data = parse_html_to_json(html_content, book, filename)
                            oct_chapters.insert_one(
                                {
                                    "book": book,
                                    "filename": filename,
                                    "sections": json_data,
                                    "order": order_counter,
                                }
                            )
                            order_counter += 1
                        except Exception as e:
                            print(f"Error while parsing {filename} html >> {e}")
                            files_with_error.insert_one(
                                {"book": book, "filename": filename, "error": e}
                            )
                            # clear mongo
                            oct_chapters.delete_many({"book": book})
                    else:
                        print("no html content found : ", filename)
                        files_with_error.insert_one(
                            {
                                "book": book,
                                "filename": filename,
                                "error": "no html content found",
                            }
                        )
                    files.append(filename)
                prev_filename = filename

    book_data = {"book": book, "extraction": "completed"}
    extracted_books.insert_one(book_data)


def find_figure_tag_in_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    div_with_figure_class = soup.find_all("div", class_="figure")
    return div_with_figure_class


def get_html_from_epub(epub_path):
    book = epub.read_epub(epub_path)
    # Iterate through items in the EPUB book
    for item in book.get_items():
        # Check if the item is of type 'text'
        if item.get_type() == ebooklib.ITEM_DOCUMENT:
            # Extract the HTML content
            html_content = item.get_content().decode("utf-8", "ignore")

            # Find figure tags in the HTML content
            figure_tags = find_figure_tag_in_html(html_content)

            # If figure tags are found, return the first one and break the loop
            if figure_tags:
                return figure_tags[0]
    # Return None if no figure tags are found
    return None



# taking books from publishers collection and checking if it has pattern (figure tag inside any html file)
# pattern2=[]
# extracted=[]
# for book in publisher_collection.find():
#     if (
#         "publishers" in book
#         and book["publishers"]
#         and book["publishers"][0].startswith("Wiley")
#     ):
#         if "s3_key" in book:
#             s3_key = book["s3_key"]
#             bookname = book["s3_key"].split("/")[-2]
#             already_extracted = extracted_books.find_one({"book": bookname})
#             if not already_extracted:
#                 print("e")
#                 epub_path = download_epub_from_s3(bookname, s3_key)
#                 if not epub_path:
#                     continue
#                 figure_tag = get_html_from_epub(epub_path)
#                 if figure_tag:
#                     if os.path.exists(epub_path):
#                         os.remove(epub_path)
#                     print("figure tag found")
#                     pattern2.append(s3_key)
#                     # get_book_data(bookname)
#                 else:
#                     print("no figure tag")
#                     if os.path.exists(epub_path):
#                         os.remove(epub_path)
#             else:
#                 print(f"this {bookname}already extracted")
#                 extracted.append(bookname)

# print("total extracted books", len(extracted))
# print("total pattern",len(pattern2))
# f=open("pattern2.txt",'w')
# f.write(str(pattern2))
extracted=[]
books=['Books/Oct29-1/Building Conflict Competent Teams (9780470189474)/9780470189474.epub', 'Books/Oct29-1/The One-Page Project Manager for IT Projects (9780470275887)/9780470275887.epub', 'Books/Oct29-1/The Data Warehouse ETL Toolkit (9780764567575)/9780764567575.epub', 'Books/Oct29-1/Effective Project Management (9780470042618)/9780470042618.epub', 'Books/Oct29-1/The Architecture of Computer Hardware Systems Software (9781118322635)/9781118322635.epub', 'Books/Oct29-1/Marketing and Finance (9781119953388)/9781119953388.epub', 'Books/Oct29-1/The Project Manager_s Guide to Mastering Agile (9781118991046)/9781118991046.epub', 'Books/Oct29-1/Managing Quality (9781405142793)/9781405142793.epub', 'Books/Oct29-1/Encyclopedia of Technology and Innovation Management (9781405160490)/9781405160490.epub', 'Books/Oct29-1/Market Segmentation (9781118432754)/9781118432754.epub', 'Books/Oct29-1/Fixed Income Securities (9781118133965)/9781118133965.epub', 'Books/Oct29-1/Cybersecurity_ Managing Systems Conducting Testing (9781118697115)/9781118697115.epub', 'Books/Oct29-1/Security Engineering (9780470068526)/9780470068526.epub', 'Books/Oct29-1/JavaScript and JQuery (9781118531648)/9781118531648.epub', 'Books/Oct29-1/Six Sigma Quality Improvement with Minitab Second Edition (9781119976189)/9781119976189.epub', 'Books/Oct29-1/Design for Embedded Image Processing on FPGAs (9780470828496)/9780470828496.epub', 'Books/Oct29-1/Computer System Designs (9780470643365)/9780470643365.epub', 'Books/Oct29-1/Rapid Instructional Design (9781118973974)/9781118973974.epub', 'Books/Oct29-1/9781118445990/9781118445990.epub', 'Books/Oct29-1/9780470947326/9780470947326.epub', 'Books/Oct29-1/Project Management Leadership (9781118825402)/9781118825402.epub', 'Books/Oct29-1/9781118738184/9781118738184.epub', 'Books/Oct29-1/9781118651766/9781118651766.epub', 'Books/Oct29-1/9781119941651/9781119941651.epub', 'Books/Oct29-1/9780470525869/9780470525869.epub', 'Books/Oct29-1/Guide to Project Management (9781118417423)/9781118417423.epub', 'Books/Oct29-1/9780470827055/9780470827055.epub', 'Books/Oct29-1/9781118757222/9781118757222.epub', 'Books/Oct29-1/Demand-Driven Forecasting (9781118735572)/9781118735572.epub', 'Books/Oct29-1/The Handbook of International Advertising Research (9781118378458)/9781118378458.epub', 'Books/Oct29-1/DAFX_ Digital Audio Effects Second Edition (9780470665992)/9780470665992.epub', 'Books/Oct29-1/Encyclopedia of Financial Models I (9781118539859)/9781118539859.epub', 'Books/Oct29-1/Operations Management (9780470525906)/9780470525906.epub', 'Books/Oct29-1/Get Your Business Funded (9781118086650)/9781118086650.epub', 'Books/Oct29-1/Structured Products in Wealth Management (9781118580400)/9781118580400.epub', 'Books/Oct29-1/Managing Innovation Design and Creativity (9780470510667)/9780470510667.epub', 'Books/Oct29-1/9781118238066/9781118238066.epub', 'Books/Oct29-1/9781118241332/9781118241332.epub', 'Books/Oct29-1/9780471201007/9780471201007.epub', 'Books/Oct29-1/9781118074398/9781118074398.epub', 'Books/Oct29-1/9781119957317/9781119957317.epub', 'Books/Oct29-1/9780470387603/9780470387603.epub', 'Books/Oct29-1/9781118923856/9781118923856.epub', 'Books/Oct29-1/9780470531907/9780470531907.epub', 'Books/Oct29-1/9781118921968/9781118921968.epub', 'Books/Oct29-1/9781119008118/9781119008118.epub', 'Books/Oct29-1/Computational Intelligence (9781118534816)/9781118534816.epub', 'Books/Oct29-1/Say Anything to Anyone Anywhere (9781118605820)/9781118605820.epub', 'Books/Oct29-1/Corporate Governance Fifth Edition (9780470972595)/9780470972595.epub', 'Books/Oct29-1/9781118594988/9781118594988.epub', 'Books/Oct29-1/Structural Bioinformatics 2nd Edition (9781118210567)/9781118210567.epub', 'Books/Oct29-1/Zero Risk Real Estate (9781118459355)/9781118459355.epub', 'Books/Oct29-1/9781444342178/9781444342178.epub', 'Books/Oct29-1/9781118469088/9781118469088.epub', 'Books/Oct29-1/Search Engine Optimization Bible Second Edition (9780470452646)/9780470452646.epub', 'Books/Oct29-1/Structured Credit Products (9781118177136)/9781118177136.epub', 'Books/Oct29-1/9781119978305/9781119978305.epub', 'Books/Oct29-1/9781119992868/9781119992868.epub', 'Books/Oct29-1/9780470560631/9780470560631.epub', 'Books/Oct29-1/9780470230190/9780470230190.epub', 'Books/Oct29-1/9781118416594/9781118416594.epub', 'Books/Oct29-1/Network Security Bible 2nd Edition (9780470502495)/9780470502495.epub', 'Books/Oct29-1/Semi-Supervised and Unsupervised Machine Learning (9781118586136)/9781118586136.epub', 'Books/Oct29-1/Software in 30 Days (9781118240908)/9781118240908.epub', 'Books/Oct29-1/Predictive Analytics (9781118416853)/9781118416853.epub', 'Books/Oct29-1/Stochastic Geometry for Image Analysis (9781118601136)/9781118601136.epub', 'Books/Oct29-1/Statistical and Machine Learning Approaches for Network Analysis (9781118346983)/9781118346983.epub', 'Books/Oct29-1/Make Difficult People Disappear (9781118283639)/9781118283639.epub', 'Books/Oct29-1/Zebras and Cheetahs (9781118644706)/9781118644706.epub', 'Books/Oct29-1/Aerosol Science_ Technology and Applications (9781118675359)/9781118675359.epub', 'Books/Oct29-1/Business Ratios and Formulas (9781118169964)/9781118169964.epub', 'Books/Oct29-1/Simulation and Modeling of Systems of Systems (9781118616956)/9781118616956.epub', 'Books/Oct29-1/Quantum Physics for Scientists and Technologists (9780470922699)/9780470922699.epub', 'Books/Oct29-1/Painless Presentations (9781118431498)/9781118431498.epub', 'Books/Oct29-1/The Power of Consistency (9781118526538)/9781118526538.epub', 'Books/Oct29-1/The Successful Frauditor_s Casebook (9781119960591)/9781119960591.epub', 'Books/Oct29-1/Business Models for the Social Mobile Cloud (9781118494196)/9781118494196.epub', 'Books/Oct29-1/iPhone® 4 Portable Genius (9780470642054)/9780470642054.epub', 'Books/Oct29-1/Introduction to Statistics Through Resampling Methods and R 2nd Edition (9781118497579)/9781118497579.epub', 'Books/Oct29-1/The Business-Oriented CIO (9780470278123)/9780470278123.epub', 'Books/Oct29-1/Gender Codes_ Why Women Are Leaving Computing (9781118035139)/9781118035139.epub', 'Books/Oct29-1/Dielectric Materials for Electrical Engineering (9781118619780)/9781118619780.epub', 'Books/Oct29-1/SolidWorks® Administration Bible (9780470537268)/9780470537268.epub', 'Books/Oct29-1/The LTE_SAE Deployment Handbook (9780470977262)/9780470977262.epub', 'Books/Oct29-1/Ultra Wide Band Antennas (9781118586570)/9781118586570.epub', 'Books/Oct29-1/LTE Security Second Edition (9781118380659)/9781118380659.epub', 'Books/Oct29-1/Radio Resource Allocation and Dynamic Spectrum Access (9781118574355)/9781118574355.epub', 'Books/Oct29-1/Access™ 2007 Bible (9780470046739)/9780470046739.epub', 'Books/Oct29-1/Project Management Accounting (9780470044698)/9780470044698.epub', 'Books/Oct29-1/LTE WiMAX and WLAN Network Design (9781119971443)/9781119971443.epub', 'Books/Oct29-1/Adobe® Photoshop® Lightroom® 2 (9780470400760)/9780470400760.epub', 'Books/Oct29-1/Financial and Managerial Accounting (9781118004234)/9781118004234.epub', 'Books/Oct29-1/Radio Engineering (9781118602225)/9781118602225.epub', 'Books/Oct29-1/Digital Sports Photography (9780764596070)/9780764596070.epub', 'Books/Oct29-1/Indoor Radio Planning (9781119973683)/9781119973683.epub', 'Books/Oct29-1/LTE – The UMTS Long Term Evolution From Theory to Practice Second Edition (9780470660256)/9780470660256.epub', 'Books/Oct29-1/SolidWorks® Surfacing and Complex Shape Modeling Bible (9780470258231)/9780470258231.epub', 'Books/Oct29-1/LTE for UMTS_ Evolution to LTE-Advanced 2nd Edition (9781119992936)/9781119992936.epub', 'Books/Oct29-1/Radio Resource Management in Multi-Tier Cellular Wireless Networks (9781118749777)/9781118749777.epub', 'Books/Oct29-1/AutoCAD® 2009 _ AutoCAD LT® 2009 Bible (9780470260173)/9780470260173.epub', 'Books/Oct29-1/AutoCAD® 2008 and AutoCAD LT® 2008 Bible (9780470120491)/9780470120491.epub', 'Books/Oct29-1/3ds Max® 2009 Bible (9780470381304)/9780470381304.epub', 'Books/Oct29-1/Autodesk® Revit® Architecture 2013 (9781118255940)/9781118255940.epub', 'Books/Oct29-1/Design of Rotating Electrical Machines 2nd Edition (9781118581575)/9781118581575.epub', 'Books/Oct29-1/Engineering Circuit Analysis (9780470873779)/9780470873779.epub', 'Books/Oct29-1/A_B Testing_ The Most Powerful Way to Turn Clicks Into Customers (9781118536094)/9781118536094.epub', 'Books/Oct29-1/The Money Compass (9781118614617)/9781118614617.epub', 'Books/Oct29-1/Leveraged Buyouts (9781118674451)/9781118674451.epub', 'Books/Oct29-1/The Handbook of Global Corporate Treasury (9781118127346)/9781118127346.epub', 'Books/Oct29-1/Event Processing for Business (9781118171851)/9781118171851.epub', 'Books/Oct29-1/The Psychology of Retirement (9781118408711)/9781118408711.epub', 'Books/Oct29-1/Painless Performance Conversations (9781118631706)/9781118631706.epub', 'Books/Oct29-1/Fedora® Linux® TOOLBOX (9780470082911)/9780470082911.epub', 'Books/Oct29-1/Audit and Assurance Essentials (9781118454169)/9781118454169.epub', 'Books/Oct29-1/Digital Infrared Photography Photo Workshop (9780470405215)/9780470405215.epub', 'Books/Oct29-1/Structural Dynamic Analysis with Generalized Damping Models (9781118863022)/9781118863022.epub', 'Books/Oct29-1/Adobe® Dreamweaver® CS5 Bible (9780470585863)/9780470585863.epub', 'Books/Oct29-1/Dreamweaver® CS3 Bible (9780470122143)/9780470122143.epub', 'Books/Oct29-1/IPSAS Explained_ A Summary of International Public Sector Accounting Standards 2nd Edition (9781118400135)/9781118400135.epub', 'Books/Oct29-1/Breakthrough IT (9780470124840)/9780470124840.epub', 'Books/Oct29-1/Space Antenna Handbook (9781119945840)/9781119945840.epub', 'Books/Oct29-1/Master Data Management in Practice (9781118085684)/9781118085684.epub', 'Books/Oct29-1/Information Technology for Management (9780470916803)/9780470916803.epub', 'Books/Oct29-1/Microgrids_ Architectures and Control (9781118720646)/9781118720646.epub', 'Books/Oct29-1/Encyclopedia of Financial Models 3 Volume Set (9781118539958)/9781118539958.epub', 'Books/Oct29-1/Probability and Statistics for Finance (9780470400937)/9780470400937.epub', 'Books/Oct29-1/Malware Analyst_s Cookbook and DVD (9780470613030)/9780470613030.epub', 'Books/Oct29-1/Computational Lithography (9781118043578)/9781118043578.epub', 'Books/Oct29-1/Financial Services Firms (9781118098530)/9781118098530.epub', 'Books/Oct29-1/Digital Signal Processing Using MATLAB for Students and Researchers (9780470880913)/9780470880913.epub', 'Books/Oct29-1/Advances in Computed Tomography for Geomaterials (9781118587614)/9781118587614.epub', 'Books/Oct29-1/You_ve Got to Be Kidding! (9781118086506)/9781118086506.epub', 'Books/Oct29-1/How to Implement Market Models Using VBA (9781118961995)/9781118961995.epub', 'Books/Oct29-1/Information Security (9781118027967)/9781118027967.epub', 'Books/Oct29-1/A Companion to New Media Dynamics (9781118321638)/9781118321638.epub', 'Books/Oct29-1/Aperture® 3 Portable Genius (9780470386729)/9780470386729.epub', 'Books/Oct29-1/Cellular Technologies for Emerging Markets (9780470975671)/9780470975671.epub', 'Books/Oct29-1/Executive Roadmap to Fraud Prevention and Internal Control (9781118235515)/9781118235515.epub', 'Books/Oct29-1/Building Flash® Web Sites For Dummies® (9780471792208)/9780471792208.epub', 'Books/Oct29-1/Canon® EOS Rebel T1i_500D Digital Field Guide (9780470521281)/9780470521281.epub', 'Books/Oct29-1/Illustrator® CS5 Bible (9780470584750)/9780470584750.epub', 'Books/Oct29-1/Accounting and Auditing Research and Databases (9781118416877)/9781118416877.epub', 'Books/Oct29-1/iConnected_ Use AirPlay iCloud (9781118673010)/9781118673010.epub']
for book in books:
    bookname=book.split("/")[-2]
    already_extracted = extracted_books.find_one({"book": bookname})
    if already_extracted:
        extracted.append(bookname)
print(len(extracted))
# get_book_data("Statistical and Machine Learning Approaches for Network Analysis (9781118346983)")
# get_book_data("The One-Page Project Manager for IT Projects (9780470275887)")
# get_book_data('The One-Page Project Manager for IT Projects (9780470275887)')
# get all books from aws and checking if it has pattern (figure tag inside any html file)
# extracted=[]
# books=get_all_books_names(bucket_name, folder_name)
# print(len(books))
# book_with_figure_tags=[]
# books_with_out_figure_tags=[]
# for book in books:
#     already_extracted = extracted_books.find_one({"book": book})
#     s3_key=f'{folder_name}{book}/{book}.epub'
#     print(s3_key)
#     if not already_extracted:
#         print('e')
#         epub_path = download_epub_from_s3(book, s3_key)
#         if not epub_path:
#             continue
#         try:
#             figure_tag = get_html_from_epub(epub_path)
#         except Exception as e:
#             print("error while identify figure tag",e)
#             continue
#         if figure_tag:
#             if os.path.exists(epub_path):
#                 os.remove(epub_path)
#                 print("figure tag found")
#                 book_with_figure_tags.append(book)
#                 get_book_data(book)
#         else:
#             print("no figure tag")
#             books_with_out_figure_tags.append(book)
#             if os.path.exists(epub_path):
#                 os.remove(epub_path)
#     else:
#         extracted.append(book)
       

# print("total books", len(books))
# print("total extracted", len(extracted))
# print("total books with figure tag",len(book_with_figure_tags))
# # f=open('wiley_aws_books_with_figure','w')
# # f.write(str(book_with_figure_tags))
# print("total books with out figure tag",len(book_with_figure_tags))
# # f=open('wiley_aws_books_without_figure','w')
# # f.write(str(books_with_out_figure_tags)

# # get_html_from_epub("/home/bud-data-extraction/datapipeline/Books/Oct29-Wiley/9780470317235.epub")