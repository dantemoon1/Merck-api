from flask import Flask
import PyPDF2
import re
import csv
import camelot

app = Flask(__name__)

@app.route('/')
def hello_world():
    return {'hello': 'world'}

@app.route('/pdf-test')
def pdf_test():
    pdfFileObj = open('BP-0001.pdf', 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pageObj = pdfReader.getPage(0)
    text = pageObj.extractText()
    return text
