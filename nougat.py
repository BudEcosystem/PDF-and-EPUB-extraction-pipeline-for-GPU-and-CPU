# import subprocess
# import psutil
# import os
# import GPUtil
# import os
# import psutil
# from PIL import Image
# from pix2tex.cli import LatexOCR
# from utils import timeit
# import requests
# @timeit
# def gettext(pdf_path):
#     process = psutil.Process(os.getpid())
#     print(f"Memory Usage: {process.memory_info().rss / (1024 ** 2):.2f} MB")
#     gpus = GPUtil.getGPUs()
#     for i, gpu in enumerate(gpus):
#         print(f"GPU {i + 1} - GPU Name: {gpu.name}")
#         print(f"  GPU Utilization: {gpu.load * 100:.2f}%")
#     try:
#         command=[
#             "nougat",
#             pdf_path,
#             "--no-skipping"

#         ]
#         result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
#         print(result.stderr)
#         print(result.stdout)
#         process = psutil.Process(os.getpid())

#         print(f"Memory Usage: {process.memory_info().rss / (1024 ** 2):.2f} MB")
#         gpus = GPUtil.getGPUs()
#         for i, gpu in enumerate(gpus):
#             print(f"GPU {i + 1} - GPU Name: {gpu.name}")
#             print(f"  GPU Utilization: {gpu.load * 100:.2f}%")
#     except Exception as e:
#         print(f"An error occurred while extracting pdf data through nougat",e)
        
# gettext("/home/azureuser/prakash2/pdf_extraction_pipeline/page_3.pdf")
  
# from PIL import Image
# from pix2tex.cli import LatexOCR
# model = LatexOCR()
# @timeit
# def getLatex():
#     img = Image.open('/home/azureuser/prakash2/cropeed2cb855e00657422c94e9c63440dc17fa.png')
   
#     latex_text=model(img)
#     print(latex_text)

# getLatex()
import requests
image_path="/home/azureuser/prakash2/90c0db0c91cb492e87f8b2903b610eb4.pdf"
files = {
        'file': (image_path, open(image_path, 'rb'))
    }
response = requests.post('http://127.0.0.1:8503/predict', files=files)
print(response)
