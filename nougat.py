import subprocess

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
    except Exception as e:
        print(f"An error occurred while extracting pdf data through nougat",e)
        
gettext("page_3.pdf")