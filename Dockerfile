FROM nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04

RUN apt-get update
RUN apt-get install -y python3.11
RUN apt-get -y install python3-pip

ARG YOUR_ENV

ENV YOUR_ENV=${YOUR_ENV} \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=1.7.1

# System deps:
RUN pip3 install "poetry==$POETRY_VERSION"

# change command to CUDA version of system
RUN pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# install pytesseract
RUN apt install -y tesseract-ocr libtesseract-dev git libgl1-mesa-glx

RUN pip3 install "git+https://github.com/facebookresearch/detectron2.git@v0.5#egg=detectron2"

RUN mkdir src
WORKDIR /src
COPY poetry.lock pyproject.toml /src/

# Project initialization:
RUN poetry config virtualenvs.create false \
    && poetry install $(test "$YOUR_ENV" == production && echo "--no-dev") --no-interaction --no-ansi

# copy project files
COPY . /src


