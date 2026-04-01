FROM quay.io/jupyter/scipy-notebook:latest

WORKDIR /home/jovyan/work

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
