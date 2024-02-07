import os
from fastapi import FastAPI, File, UploadFile, Response, status
from http import HTTPStatus
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from docxtract import (
    LayoutAnalysisPipeline,
    LayoutAnalysisOnnxPipeline,
    LayoutAnalysisOVPipeline,
    NougatLaTexOCRPipeline,
    Pix2TexPipeline,
    Pix2TexOnnxPipeline,
    NougatOCRPipeline,
    NougatOCROnnxPipeline,
)
import torch
from dotenv import load_dotenv
from PIL import Image
from io import BytesIO
from utils import get_gpu_device_id
Image.MAX_IMAGE_PIXELS = None
load_dotenv()


model = None
model_name = os.environ.get("model_name", None)

model_registry = {
    "ptm": LayoutAnalysisPipeline,
    "ptm_onxx": LayoutAnalysisOnnxPipeline,
    "ptm_ov": LayoutAnalysisOVPipeline,
    "nougat": NougatOCRPipeline,
    "nougat_onnx": NougatOCROnnxPipeline,
    "nougat_latex": NougatLaTexOCRPipeline,
    "pix2tex": Pix2TexPipeline,
    "pix2tex_onnx": Pix2TexOnnxPipeline
}

pretrained_model_name_or_path = os.environ.get("pretrained_model_name_or_path", None)

device = os.environ.get("device", "cpu")
if device == "gpu":
    device_id = get_gpu_device_id()
    device = f"cuda:{device_id}"
    device = "cuda:2"

precision = os.environ.get("model_precision", None)

@asynccontextmanager
async def lifespan2(app: FastAPI):
    # Load the ML model pipeline
    global model, pipeline
    if model is None:
        print("here "*5)
        if model_name is None:
            yield
        model = model_registry.get(model_name, None)
        kwargs = {}
        if pretrained_model_name_or_path is None:
            yield
        kwargs["pretrained_model_name_or_path"] = pretrained_model_name_or_path
        kwargs["device"] = device
        if precision:
            kwargs["precision"] = precision
        print(kwargs)
        pipeline = model.from_pretrained(**kwargs)      
    yield
    # Clean up the ML models and release the resources
    del pipeline
    torch.cuda.empty_cache()


app = FastAPI(title="Model API Server", lifespan=lifespan2)
origins = ["http://localhost", "http://127.0.0.1"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health Check Route
@app.get("/health")
async def health_check():
    response = {
        "status-code": HTTPStatus.OK,
        "data": {},
    }
    return response

# Model Inference Routes
@app.post("/publaynet")
async def publaynet_inference():
    # Perform PublayNet model inference
    # Add your code here
    return {"result": "PublayNet inference result"}

@app.post("/tablebank")
async def tablebank_inference():
    # Perform TableBank model inference
    # Add your code here
    return {"result": "TableBank inference result"}

@app.post("/math_formula_detection")
async def math_formula_detection_inference():
    # Perform Math Formula Detection model inference
    # Add your code here
    return {"result": "Math Formula Detection inference result"}

@app.post("/nougat")
async def nougat_inference(
    response: Response,
    files: list[UploadFile] = File(...)):
    outputs = []
    images = []
    for file in files:
        # Read the contents of the uploaded file as bytes
        file_contents = await file.read()
        # print(type(file_contents))
        # Use PIL to open the image from bytes
        image = Image.open(BytesIO(file_contents))
        images.append(image)
    try:
        outputs = pipeline(images=images)
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_400_BAD_REQUEST
    return outputs

@app.post("/nougat-latex-ocr")
async def nougat_latex_ocr_inference():
    # Perform Nougat Latex OCR model inference
    # Add your code here
    return {"result": "Nougat Latex OCR inference result"}

@app.post("/pix2tex")
async def pix2tex_inference():
    # Perform Pix2Tex model inference
    # Add your code here
    return {"result": "Pix2Tex inference result"}


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("app:app", host='127.0.0.1', port=7777, reload=True)