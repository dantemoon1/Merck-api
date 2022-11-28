FROM python:3.9
ADD requirements.txt /requirements.txt
ADD main.py /main.py
ADD okteto-stack.yaml /okteto-stack.yaml
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y python3-opencv
RUN apt-get install -y ghostscript
RUN pip install opencv-python
EXPOSE 8080
COPY ./app app
CMD ["python3", "main.py"]
