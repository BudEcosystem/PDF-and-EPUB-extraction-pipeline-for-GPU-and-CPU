# PDF-Data-Extraction-Pipeline
There 8 consumer files that you need to run

```bash 
python pdf_extraction_pipeline/pdf_pipeline/book_completion_consumer.py
```

```bash 
python pdf_extraction_pipeline/pdf_pipeline/nougat_consumer.py
```

```bash 
python3 pdf_extraction_pipeline/pdf_pipeline/check_ptm_comsuer.py
```

```bash 
python pdf_extraction_pipeline/pdf_pipeline/pdfigCap_consumer.py 
```

```bash 
python pdf_extraction_pipeline/pdf_pipeline/mfd_consumer.py
```

```bash 
python pdf_extraction_pipeline/pdf_pipeline/tableBank_consumer.py 
```

```bash 
python pdf_extraction_pipeline/pdf_pipeline/publeynet_consumer.py
```

```bash 
python pdf_extraction_pipeline/pdf_pipeline/pdfconsumer.py 
```
### Run Producer file
Before running producer file, you should have some books in your mongodb collection.

 ```bash 
python pdf_extraction_pipeline/pdf_pipeline/pdf_producer.py  
```



