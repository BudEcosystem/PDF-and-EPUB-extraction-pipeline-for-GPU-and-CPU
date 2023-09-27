import subprocess
from utils import timeit

@timeit
def process_pdf_with_nougat(input_pdf_path):
    try:
        # Construct the command as a list of arguments
        command = [
            "nougat",
            input_pdf_path,
            "--no-skipping"
        ]
        # Run the command and capture its output
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Print the standard output and error
        print("Standard Output:")
        print(result.stdout)

        if result.returncode == 0:
            print("PDF processing with nougat completed successfully.")
        else:
            print("PDF processing with nougat failed.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage
input_pdf_path = "/home/bud-data-extraction/datapipeline/page_1.pdf"
process_pdf_with_nougat(input_pdf_path)



