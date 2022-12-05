#from typing import Union
from fastapi import FastAPI, Response, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
#from deta import Deta, Drive
from dotenv import load_dotenv
import os
import json

#imports for parsing
import PyPDF2
import re
import csv
import camelot
import pandas as pd


app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()

#get the current working directory
cwd = os.getcwd()


@app.get("/", response_class=HTMLResponse)
def render():
    #print all the folders in the current directory
    #print(os.listdir())

    #check if the current directory is the base directory and if not, change it to the base directory
    print('the default directory is: ', cwd)
    print('the current directory is: ', os.getcwd())
    if cwd != os.getcwd():
        os.chdir(cwd)
        print('the current directory is: ', os.getcwd())

    return """
    <form action="/upload" method="post" enctype="multipart/form-data">
        <input type="file" name="file">
        <input type="submit">
    </form>
    """

@app.post("/upload")
def upload(file: UploadFile = File(...)):
    #upload file to /tmp
    with open(f"/tmp/{file.filename}", "wb") as buffer:
        buffer.write(file.file.read())
    #get the name of the file from tmp
    file_name = f"tmp/{file.filename}"
    #redirect to the root route
    #return {"filename": file.filename}
    return RedirectResponse(url="/parse", status_code=303)

@app.get("/parse")
def parse_pdf():

    os.chdir("/tmp") #change directory to /tmp

    #check if the csv already exists, if so return it as a response
    if os.path.exists('BP-0001.csv'):
        print('csv already exists on server')
        return Response(pd.read_csv('BP-0001.csv').to_json(orient='records'), media_type='application/json')

    inputFile = "BP-0001.pdf"

    pdfFileObj = open(inputFile,'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj) 

    searchString = ""
    numOfPages = pdfReader.numPages
    for i in range(numOfPages):
        pageObj = pdfReader.getPage(i)
        z = pageObj.extractText()
        document = z.replace("\n", " ").replace(",","")
        document = re.sub(' +',' ', document)
        searchString = searchString + document

    pdfFileObj.close()

    # Table Extraction

    tables = camelot.read_pdf(inputFile,pages = "1-" + str(numOfPages),line_scale = 40)

    # RegEx Parsing

    headers = []
    rows = []
    mainCompound = []

    # List Values

    # Species
    species = [" Rat ", " Monkey ", " Dog ", " Cat ", " Rabbit ", " Human "]
    # Matrix
    matrix = [" Plasma", " Urine", " Serum", " Liver", " Brain"]
    # Extraction Method
    extraction_method = [" protein precipitation ", " liquid liquid extraction ", " solid phase extraction ", " immunoprecipitation ", " solid liquid extraction "]
    # Chromatography 
    chromatography = [" reversed phase ", " normal phase "," ion exchange "," hydrophilic interaction "," liquid chromatography "]
    # Ionization Method
    ionization_method = [" turbo ionspray ", " Atmospheric pressure chemical ionization "]
    # Polarity
    polarity = [" positive ", " negative "]
    # Regression Model
    regression_model = [" linear "," quadratic "]
    # Weighting
    weighting = ["1/x","1/x2"]
    # Dilutent
    dilutent = ["ACN/H 2O \[50/50\]","MeOH/H 2O \[50/50\]","DMSO"]
    # Solution and Sample Storage Temp
    storage_temp = ["-70°C","-20°C","\+4°C","room temperature"]
    # Anticoagulant
    anticoagulant = ["EDTA","Na Heparin"]

    # Methods

    def setIfNotNone(searchInput):
        if searchInput != None:
            return searchInput.group(1)
        return searchInput

    def compoundFindSearch(value, compoundType):
        value = None
        value = re.search(compoundType + "-([^\s]+)",searchString, re.I)
        value = setIfNotNone(value)
        headers.append(compoundType)
        rows.append(compoundType + "-" + str(value.replace(")","")))

    def valueFindSearchList(value, nameType):
        returnValue = None
        for a in value:
            returnValue = re.search('(' + a + ')', searchString, re.I)
            if(returnValue != None):
                returnValue = setIfNotNone(returnValue)
                headers.append(nameType)
                rows.append(returnValue)
                return
        headers.append(nameType)
        rows.append(returnValue)

    # Write another method to add it to rows

    bpNumber = compoundFindSearch("BP Number","BP")

    # speciesValue = valueFindSearchList(species, "Species") [happens to be wrong sometimes]
    matrixValue = valueFindSearchList(matrix, "Matrix")
    extractionMethodValue = valueFindSearchList(extraction_method, "Extraction Method")
    chromatographyValue = valueFindSearchList(chromatography, "Chromatography")
    ionizationMethodValue = valueFindSearchList(ionization_method, "Ionization Method")
    polarityValue = valueFindSearchList(polarity, "Polarity")
    regressionModelValue = valueFindSearchList(regression_model, "Regression Model")
    weightingValue = valueFindSearchList(weighting, "Weighting")
    dilutentValue = valueFindSearchList(dilutent, "Dilutent")
    anticoagulantValue = valueFindSearchList(anticoagulant, "Anticoagulant")

    # LLOQ

    LLOQ = None
    LLOQ = re.search(r"\(LLOQ\) for this method is (.*?) with", searchString, re.I)
    LLOQ = setIfNotNone(LLOQ)
    headers.append("LLOQ")
    rows.append(LLOQ)

    # Calibration Range

    calibrationRange = None
    calibrationRange = re.search(r"calibration range from (.*?) to ([^\s]+)", searchString, re.I)
    headers.append("Calibration Range From")
    rows.append(calibrationRange.group(1))
    headers.append("Calibration Range To")
    rows.append(calibrationRange.group(2))

    # Matrix Sample Volume

    matrixSampleVolume = None
    matrixSampleVolume = re.search(r"using a (.*?) ", searchString)
    headers.append("Matrix Sample Volume")
    rows.append(matrixSampleVolume.group(1))

    # Storage Temperature
    temperatureList = []

    for temperatures in storage_temp:
        tempTemperature = re.search('(' + temperatures + ')', searchString, re.I) 
        if tempTemperature != None:
            temperatureList.append(tempTemperature.group(1))

    if len(temperatureList) >= 2:
        headers.append("Storage Temperature Standard Solutions")
        rows.append(temperatureList[0])
        headers.append("Storage Temperature Matrix Sample")
        rows.append(temperatureList[1])

    # Special Requirments

    specialRequirments = None
    if re.search(r"Special Requirements", searchString, re.I):
        specialRequirments = re.search(r"Special Requirements(.*?)1 INSTRUMENTATION", searchString, re.I)
    if specialRequirments != None:
        headers.append("Special Requirements")
        rows.append(specialRequirments.group(1).replace(":",""))

    print(tables)

    # Camelot Parsing

    # Table Extraction Methods

    # Extracts Values Given List of Rows and Columns [Named After Column]
    def findColumnRowValues(searchTable, searchColumns, searchRows):
        rowNumberList = []
        columnNumberList = []

        # Gets Position of Column
        for column in range(len(searchTable[0])):
            for columnValue in searchColumns:
                if columnValue in searchTable[0][column]:
                    columnNumberList.append(column)

        # Gets Position of Row
        for row in range(len(searchTable)):
            for rowValue in searchRows:
                if rowValue in searchTable[row][0]:
                    rowNumberList.append(row)

        # Iterates over Positions and Adds It [header is [row][0] since inputted value names are there]
        for row in rowNumberList:
            for column in columnNumberList:
                headers.append(searchTable[row][0])
                rows.append(searchTable[row][column])

    # Get and labels values from Rows under Header [Ions Monitored] - Labeling Based on Column 1
    def findLabelRowValues(searchTable):
        for row in range(len(searchTable)):
            if row != 0:
                for column in range(len(searchTable[row])):
                    if column == 0:
                        if "SIL" in searchTable[row][column]:
                            headers.append("IS Number")
                            rows.append(str(searchTable[row][column]))
                        elif "L-" in searchTable[row][column]:
                            headers.append("L Number")
                            rows.append(str(searchTable[row][column]))
                        elif "MK-" in searchTable[row][column]:      
                            headers.append("MK Number")
                            rows.append(str(searchTable[row][column]))              
                    if column != 0:
                        headers.append(searchTable[row][0] + " " + str(searchTable[0][column]))
                        rows.append(searchTable[row][column])

    # Get and label values from Rows under Header [Analyte] - Labeling not Based on Column 1
    def findAllValuesUnderColumn(searchTable):
        for row in range(len(searchTable)):
            if row != 0:
                for column in range(len(searchTable[row])):
                    headers.append(searchTable[0][column])
                    rows.append(searchTable[row][column])

    def findLabelRowValuesButTextIsTooClose(searchTable, searchColumns):
        for row in range(len(searchTable)):
            if row != 0:
                for column in range(len(searchTable[row])):
                    if column == 0:
                        if "SIL" in searchTable[row][column]:
                            headers.append("IS Number")
                            rows.append(str(searchTable[row][column]))
                        elif "L-" in searchTable[row][column]:
                            headers.append("L Number")
                            rows.append(str(searchTable[row][column]))
                        elif "MK-" in searchTable[row][column]:      
                            headers.append("MK Number")
                            rows.append(str(searchTable[row][column]))              
                    if column != 0:
                        headers.append(searchTable[row][0] + " " + searchColumns[column - 1])
                        rows.append(searchTable[row][column])

    counter = 0

    for table in tables:
        if "Category" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")) and "Mass Spectrometer" in re.sub(' +',' ', table.df.at[1,0].replace("\n", "")):
            columnValues = ["Components"]
            rowValues = ["Mass Spectrometer", "Liquid Handling"]
            findColumnRowValues(table.df.to_numpy(), columnValues, rowValues)
        elif "Category (General)" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            l = len(table.df.to_numpy())
            headers.append("Column Name")
            rows.append(table.df.to_numpy()[1][0])
            headers.append("Column Manufacturer/Supplier")
            rows.append(table.df.to_numpy()[1][1])
        elif "Category (Equipment)" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            columnValues = ["Manufacturer"]
            rowValues = ["Microbalance", "Analytical Balance", "Refrigerated centrifuge", "pH Meter", "Plate sealer"]
            findColumnRowValues(table.df.to_numpy(), columnValues, rowValues)
        elif "Category (Pipettes)" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            columnValues = ["Manufacturer"]
            rowValues = ["Adjustable Pipettes", "Pipette Tips"]
            findColumnRowValues(table.df.to_numpy(), columnValues, rowValues)
        elif "Category (Automation Supplies)" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            columnValues = ["Manufacturer"]
            rowValues = ["Reagent Troughs", "Automated Workstation Tips "]
            findColumnRowValues(table.df.to_numpy(), columnValues, rowValues)
        elif "Category" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")) and "Analyte / L-Number" in re.sub(' +',' ', table.df.at[1,0].replace("\n", "")):
            findLabelRowValues(table.df.to_numpy())
        elif "Species" in re.sub(' +',' ', table.df.at[0,1].replace("\n", "")) and "Anticoagulant" in re.sub(' +',' ', table.df.at[0,2].replace("\n", "")):
            findAllValuesUnderColumn(table.df.to_numpy())
        elif "Standard Solution ID" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            headers.append(re.sub(' +',' ', table.df.at[0,0].replace("\n", "")))
            rows.append(table.df.to_numpy())
            # table.df.to_csv(str(table.df.at[0,0].replace("\n", "")) + '.csv')
        elif "QC Solution ID" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            headers.append(re.sub(' +',' ', table.df.at[0,0].replace("\n", "")))
            rows.append(table.df.to_numpy())
            # table.df.to_csv(str(table.df.at[0,0].replace("\n", "")) + '.csv') 
        elif "QC ID" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            headers.append(re.sub(' +',' ', table.df.at[0,0].replace("\n", "")))
            rows.append(table.df.to_numpy())
            # table.df.to_csv(str(table.df.at[0,0].replace("\n", "")) + '.csv')
        elif "Step" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            headers.append(re.sub(' +',' ', table.df.at[0,0].replace("\n", "")))
            rows.append(table.df.to_numpy())
            # table.df.to_csv(str(table.df.at[0,0].replace("\n", "")) + '.csv')  
        elif "UPLC" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")) or "HPLC" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")) or "UHPLC" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            columnValues = ["Settings"]
            rowValues = ["Elution", "Mobile Phase A", "Mobile Phase B"]  
            findColumnRowValues(table.df.to_numpy(), columnValues, rowValues)
            # table.df.to_csv(str(table.df.at[0,0]) + '.csv')
        elif "MS" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            columnValues = ["Settings"]
            rowValues = ["Ion Source", "Ion Mode", "Q1/Q3 Resolutions", "Scan Type", "Ionization Potential(IS)", "Temperature", "Curtain Gas - N2*", "GS 1 - N2*", "GS 2 - N2* ", "CAD - N2*", "MR pause between mass range", "MS settling time"]
            findColumnRowValues(table.df.to_numpy(), columnValues, rowValues)
        elif "Ions Monitored" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            searchColumns = ["Q1 m/z", "Q3 m/z", "Dwell (ms)", "DP (V)", "EP (V)", "CE (V)", "CXP (V)"]
            findLabelRowValuesButTextIsTooClose(table.df.to_numpy(), searchColumns)
        elif "Analyte" in re.sub(' +',' ', table.df.at[0,0].replace("\n", "")):
            findAllValuesUnderColumn(table.df.to_numpy())

    # Names
    names = None
    names = re.search(r"drug concentration.(.*?)APPENDIX", searchString)
    names = setIfNotNone(names)
    headers.append("Names")
    rows.append(re.sub("Page [0-9]{2}", "", re.sub("BP-[0-9]{4}", "", names)).replace("Confidential",""))

    # Prints Headers [Label] and Rows [Value (only one row)]
    print(headers)
    print(rows)

    with open(str(inputFile)[:len(inputFile)-4] + ".csv", "w", encoding = 'UTF8', newline = '') as f:
        write = csv.writer(f)
        write.writerow(headers)
        write.writerow(rows)


    return Response(pd.read_csv('BP-0001.csv').to_json(orient='records'), media_type='application/json')
    # return the csv as json without pandas
    #return Response(json.dumps(rows), media_type='application/json')

@app.get("/pdftest")
def pdfTest():
    #open the pdf located in /tmp and return the first page text
    inputFile = "BP-0001.pdf"
    pdfFileObj = open(f"tmp/{inputFile}", 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pageObj = pdfReader.getPage(0)
    return(pageObj.extractText())

@app.get("/allfiles")
def allfiles():
    #get all files in /tmp
    return {"files":os.listdir("/tmp")}

@app.get("/directorytest")
def directoryTest():
    cwd = os.getcwd()
    os.chdir("/tmp")
    nwd = os.getcwd()
    return {"cwd":cwd, "nwd":nwd}
