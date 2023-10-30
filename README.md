## PDF-Data-Extraction-Pipeline

# Setup

# Requirements.
OS - Linux, Mac
Python -  > 3.8

**Step 1.**
Create the virtual environment: Use the python3 -m venv command to create a virtual environment. Replace your_env_name with the name you want to give to your virtual environment:

`python3 -m venv your_env_name`

**Step 2.**
Activate the virtual environment: You need to activate the virtual environment to start using it. Use the following command:

`source your_env_name/bin/activate`

**Step 3.**
Clone pdf_extraction_pipeline repo

`git clone https://github.com/BudEcosystem/pdf_extraction_pipeline.git`

**Step 4.**
Change directory using following command

`cd .\pdf_extraction_pipeline`

**Step 5.**
Installation, run requirements.txt file to install required packages

`pip install -r requirements.txt`

**Step 6.**
Create .env file inside pdf_extraction_pipeline folder and copy the key content of example.eve to .env file
open .env and modify the environment variables 

**Step 7**
Run process_pdf.py file

`python process_pdf.py`


**note** 
if you are getting any installation error, then you can manully install package and model one by one
for model installtion pls refere below mentiond website

(https://layout-parser.readthedocs.io/en/latest/notes/installation.html)

after installing model you can installed required packages one by one using pip install package_name
