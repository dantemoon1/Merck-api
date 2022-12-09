#from typing import Union
from fastapi import FastAPI, Response, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
from app.internal.GraphPopulator import GraphPopulator
from neo4j import GraphDatabase
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

origins = ["*"] #currently allowing all origins, change this if you want to restrict access

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv() #not currently using but should probably store the neo4j credentials in a .env file moving forward

#get the current working directory
cwd = os.getcwd()


@app.get("/", response_class=HTMLResponse)
def render():
    #check if the current directory is the base directory and if not, change it to the base directory
    print('the default directory is: ', cwd)
    print('the current directory is: ', os.getcwd())
    if cwd != os.getcwd():
        os.chdir(cwd)
        print('the current directory is: ', os.getcwd())

    return """
    <form action="/upload" method="post" enctype="multipart/form-data">
        <h1>Upload a PDF and Parse</h1>
        <p> If you see a JSON response then the file was sucessfully parsed </p>
        <input type="file" name="file">
        <input type="submit">
    </form>
    <hr>
    <form action="/uploadcsv" method="post" enctype="multipart/form-data">
        <h1>Upload a CSV and Send to Neo4j</h1>
        <p> You should see a JSON response with "success" if the file was sucessfully sent to Neo4j </p>
        <p> Otherwise check the python console for errors </p>
        <input type="file" name="file">
        <input type="submit">
    </form>
    <hr>
    <form action="/clearcache" method="post" enctype="multipart/form-data">
        <h1>Clear the Cache</h1>
        <p> Parsed pdfs are stored on the server so if you want to actually parse a new PDF with the same name, you need to clear the cache </p>
        <input type="submit" value="Clear Cache">
    </form>
    """


@app.post("/upload")
def upload(file: UploadFile = File(...)):
    #upload file to /tmp
    with open(f"/tmp/{file.filename}", "wb") as buffer:
        buffer.write(file.file.read())
    return RedirectResponse(url="/parse/" + file.filename, status_code=303)

@app.post("/uploadcsv")
def uploadcsv(file: UploadFile = File(...)):
    with open(f"/tmp/{file.filename}", "wb") as buffer:
        buffer.write(file.file.read())
    return RedirectResponse(url="/populatedb/" + file.filename, status_code=303)

@app.post("/clearcache")
def clearcache():
    #clear the cache
    os.chdir("/tmp")
    for file in os.listdir():
        if file.endswith(".csv"):
            os.remove(file)
    
    os.chdir(cwd)

    return {"success": "cache cleared"}
    

@app.get("/parse/{file_name}")
def parse_pdf(file_name):

    os.chdir("/tmp") #change directory to /tmp

    #check if the csv already exists, if so return it as a response
    if os.path.exists(file_name.replace("pdf","csv")):
        print('csv already exists on server')
        return Response(pd.read_csv(file_name.replace("pdf","csv")).to_json(orient='records'), media_type='application/json')

    inputFile = file_name

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
    #rows.append(re.sub("Page [0-9]{2}", "", re.sub("BP-[0-9]{4}", "", names)).replace("Confidential",""))
    #this line breaks the entire thing

    # Prints Headers [Label] and Rows [Value (only one row)]
    print(headers)
    print(rows)

    with open(str(inputFile)[:len(inputFile)-4] + ".csv", "w", encoding = 'UTF8', newline = '') as f:
        write = csv.writer(f)
        write.writerow(headers)
        write.writerow(rows)

    #THIS IS WHAT TAKES THE CSV AND RETURNS IT AS JSON
    return Response(pd.read_csv(file_name.replace("pdf","csv")).to_json(orient='records'), media_type='application/json')

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


@app.get("/search/{table}") #used to search for a specific table, obviously this is not the best way to do this
def search(table):
    #1 = matrixQC
    #2 = workingStandardSolution
    #3 = workingQCSolution
    uri = "neo4j+s://d0dc487d.databases.neo4j.io"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "TPtTcrMAhv1fW93bF30hsZsPKw5x7XbfN8O8ayFvCok"))
    #make a new object
    obj = {}
    
    def print_MatrixQCs(tx):
        result = tx.run("MATCH (a:MatrixQCs) RETURN a.Table")
        for record in result:
            print(record.get('a.Table').replace("*","'"))
            obj["table"] = record.get('a.Table').replace("*","'")

    def print_WorkingStandardSolution(tx):
        result = tx.run("MATCH (a:WorkingQCSolution) RETURN a.Table")
        for record in result:
            print(record.get('a.Table').replace("*","'"))
            obj["table"] = record.get('a.Table').replace("*","'")

    def print_WorkingQCSolution(tx):
        result = tx.run("MATCH (a:WorkingStandardSolution) RETURN a.Table")
        for record in result:
            print(record.get('a.Table').replace("*","'"))
            obj["table"] = record.get('a.Table').replace("*","'")

    with driver.session() as session:
        if table == "1":
            session.execute_read(print_MatrixQCs)
        elif table == "2":
            session.execute_read(print_WorkingStandardSolution)
        elif table == "3":
            session.execute_read(print_WorkingQCSolution)
        #session.execute_read(print_WorkingStandardSolution)
        #session.execute_read(print_WorkingQCSolution)
        #session.execute_read(print_MatrixQCs)

    driver.close()
    
    return {"table":obj["table"]}




@app.get('/populatedb/{inputFile}')
def populateDB(inputFile):
    os.chdir(cwd)
    print('populating db')
    df = pd.read_csv("/tmp/"+inputFile)
    print('located the file ' + inputFile + ' in /tmp')
    schema = pd.read_csv("app/internal/schema/BP_triple_final.csv")
    print('located the schema in /schema')
    schema = schema.set_index(['node1', 'node2'])
    schema_dict = schema.to_dict("index")

    info = df.iloc[0]
    gp = GraphPopulator("neo4j+s://d0dc487d.databases.neo4j.io",
                        "neo4j", "TPtTcrMAhv1fW93bF30hsZsPKw5x7XbfN8O8ayFvCok")

    info["Matrix QC ID"] = info["Matrix QC ID"].replace("'", "*")
    info["Mixed Intermediate Standard Solution ID"] = info["Mixed Intermediate Standard Solution ID"].replace(
        "'", "*")
    info["Working Standard Solution ID"] = info["Working Standard Solution ID"].replace(
        "'", "*")
    info["Mixed Intermediate QC Solution ID"] = info["Mixed Intermediate QC Solution ID"].replace(
        "'", "*")
    info["Working QC Solution ID"] = info["Working QC Solution ID"].replace(
        "'", "*")
    info["Step"] = info["Step"].replace("'", "*")
    nodes = []


    def add_node(label, properties):
        try:
            props = {}
            for k, v in properties.items():
                if not v in info.index:
                    print(v)
                props[k] = info[v]
            nodes.append((label, props))
        except:
            print("could not create node " + label)
            print(properties)


    bp = {"BP_number": "BP"}
    add_node("BP", bp)
    if "Special Requirements" in info.index:
        nodes.append(("SpecialRequirements", {
                    "Requirements": info["Special Requirements"].split(" • ")[1:]}))
    add_node("BiologicalMatrix", {"BP_number": "BP", "Matrix": "Matrix", "Species": "Species", "Anticoagulant": "Anticoagulant",
            "Extraction_Method": "Extraction Method", "Storage_temp": "Storage Temperature Matrix Sample", "Supplier": "Supplier"})
    add_node("OperatingParameters", bp)
    add_node("UPLCParameters", {"Chromatography_Method": "Chromatography", "Elution": "Elution",
            "Mobile_phase_A": "Mobile Phase A", "Mobile_phase_B": "Mobile Phase B"})
    add_node("MassSpectrometer", {"Name": "Mass Spectrometer"})
    add_node("MSParameters", {"Ion_Source": "Ionization Method", "Ion_Mode": "Polarity", "Q1Q3_resolutions": "Q1/Q3 Resolutions", "Scan_type": "Scan Type",
            "Temperature": "Temperature", "MR_pause_between_mass_range": "MR pause between mass range", "MS_settling_time": "MS settling time"})
    add_node("CalculationParameters", {"Regression_model": "Regression Model", "Weighting": "Weighting",
            "Calibration_range_lwr": "Calibration Range From", "Calibration_range_upr": "Calibration Range To"})
    add_node("StandardPreparation", bp)
    add_node("StockStandardSolution", {"Diluent": "Dilutent", "Standard_matrix_volume":
            "Matrix Sample Volume", "Storage_temp": "Storage Temperature Standard Solutions"})
    add_node("MatrixQCs", {"LLOQ": "LLOQ", "Table": "Matrix QC ID"})
    add_node("Item", {"Name": "Column Name"})
    add_node("Supplier", {"Name": "Column Manufacturer/Supplier"})
    nodes.append(("Item", {"Name": "Microbalance"}))
    nodes.append(("Item", {"Name": "Analytical Balance"}))
    nodes.append(("Item", {"Name": "Refrigerated centrifuge - 96-well"}))
    nodes.append(("Item", {"Name": "Refrigerated centrifuge"}))
    nodes.append(("Item", {"Name": "Plate Sealer"}))
    nodes.append(("Item", {"Name": "Adjustable Pipettes"}))
    nodes.append(("Item", {"Name": "Pipette Tips"}))
    nodes.append(("Item", {"Name": "Reagent Troughs"}))
    add_node("Supplier", {"Name": "Microbalance"})
    add_node("Supplier", {"Name": "Analytical Balance"})
    add_node("Supplier", {"Name": "Refrigerated centrifuge - 96-well"})
    add_node("Supplier", {"Name": "Refrigerated centrifuge"})
    add_node("Supplier", {"Name": "Plate sealer"})
    add_node("Supplier", {"Name": "Adjustable Pipettes"})
    add_node("Supplier", {"Name": "Pipette Tips"})
    add_node("Supplier", {"Name": "Reagent Troughs"})
    # analyte
    add_node("Compound", {"Lnumber": "Analyte / L-Number Parent Drug \n(Analyte)", "Form": "Form Parent Drug \n(Analyte)",
            "Molecular_weight": "Molecular Weight \n(free form) Parent Drug \n(Analyte)", "Watson_ID": "Watson ID Parent Drug \n(Analyte)"})
    # internal standard
    add_node("Compound", {"Lnumber": "Analyte / L-Number Internal Standard (IS)", "Form": "Form Internal Standard (IS)",
            "Molecular_weight": "Molecular Weight \n(free form) Internal Standard (IS)", "Watson_ID": "Watson ID Internal Standard (IS)"})
    # epimer
    add_node("Compound", {"Lnumber": "Analyte / L-Number Epimer \n(Analyte)", "Form": "Form Epimer \n(Analyte)",
            "Molecular_weight": "Molecular Weight \n(free form) Epimer \n(Analyte)", "Watson_ID": "Watson ID Epimer \n(Analyte)"})
    add_node("MixedIntermediateStandardSolution", {
            "Table": "Mixed Intermediate Standard Solution ID"})
    add_node("WorkingStandardSolution", {"Table": "Working Standard Solution ID"})
    add_node("MixedIntermediateQCSolution", {
            "Table": "Mixed Intermediate QC Solution ID"})
    add_node("WorkingQCSolution", {"Table": "Working QC Solution ID"})
    add_node("Procedure", {"Steps": "Step"})
    ion_mon_props = {"Values": ["Q1 m/z", "Q3 m/z",
                                "Dwell (ms)", "DP (V)", "EP (V)", "CE (V)", "CXP (V)"]}
    ion_mon_props["Analyte"] = [info[info["MK Number"] + " " + val]
                                for val in ion_mon_props["Values"]]
    ion_mon_props["Internal_standard_analyte"] = [
        info["SIL-" + info["MK Number"] + " " + val] for val in ion_mon_props["Values"]]
    if "Analyte / L-Number Epimer \n(Analyte)" in info.index:
        ion_mon_props["Epimer"] = [
            info[info["Analyte / L-Number Epimer \n(Analyte)"] + " " + val] for val in ion_mon_props["Values"]]
        ion_mon_props["Internal_standard_epimer"] = [
            info["SIL-" + info["Analyte / L-Number Epimer \n(Analyte)"] + " " + val] for val in ion_mon_props["Values"]]
    nodes.append(("IonsMonitored", ion_mon_props))
    add_node("SystemSuitability", {"Analyte": "Analyte", "Peak_height": "Peak Height", "Retention_time": "Retention time \n(min)",
            "Retention_difference": "Retention time \ndifference for \nMK-0011 and \nL-000000009 \n(min)"})

    gp.execute(operation="wipe")
    for node in nodes:
        gp.execute(operation="create_node", node_label=node[0], node_props=node[1])

    for node1 in nodes:
        for node2 in nodes:
            relation = schema_dict.get((node1[0], node2[0]))
            if relation:
                gp.execute(operation="create_rel", node1_label=node1[0], node1_props={},
                        node2_label=node2[0], node2_props={}, rel_name=relation['relation'], rel_props={})
                

    gp.close()
    print("Done")
    return("Success")
