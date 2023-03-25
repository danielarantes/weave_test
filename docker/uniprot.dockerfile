FROM python:3.10.10-slim-bullseye

RUN apt update && apt upgrade -y && mkdir /build


COPY requirements.txt /build

RUN pip install -r /build/requirements.txt && pip freeze
