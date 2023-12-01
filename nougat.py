import subprocess
import psutil
import os
import GPUtil
import os
import psutil
from utils import timeit

@timeit
def gettext(pdf_path):
    try:
        command=[
            "nougat",
            pdf_path,
            "--no-skipping"
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stderr)
        print(result.stdout)
        process = psutil.Process(os.getpid())

        print(f"Memory Usage: {process.memory_info().rss / (1024 ** 2):.2f} MB")
        gpus = GPUtil.getGPUs()
        for i, gpu in enumerate(gpus):
            print(f"GPU {i + 1} - GPU Name: {gpu.name}")
            print(f"  GPU Utilization: {gpu.load * 100:.2f}%")
    except Exception as e:
        print(f"An error occurred while extracting pdf data through nougat",e)
        
gettext("page_3.pdf")