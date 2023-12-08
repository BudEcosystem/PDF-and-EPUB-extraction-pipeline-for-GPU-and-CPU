import os

directory_path = os.path.dirname(os.path.abspath(__file__))
print(directory_path)
img_dir = directory_path+'/../images'
if not os.path.exists(img_dir):
    os.makedirs(img_dir)
    print("pdf_pipeline directory exists")
