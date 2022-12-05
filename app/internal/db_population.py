from GraphPopulator import GraphPopulator
import pandas as pd

df = pd.read_csv("/tmp/BP-0003.csv")
schema = pd.read_csv("./schema/BP_triple_final.csv")
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
