import subprocess
def nougat(pdf_path):
    try:
        command=[
            "nougat",
            pdf_path,
            "--no-skipping"
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
        return result.stdout

    except Exception as e:
        print(f"error nougat: {str(e)}")

input_path='page_3.pdf'
nougat(input_path)