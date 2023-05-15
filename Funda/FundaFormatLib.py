import json
import os
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'

def parseFundaData(inputFile: str, outputFile: str):
    with open(inputFile) as input:
        inputDict = json.load(input)
    outputArray = []
    for entry in inputDict:
        outputDict = {}
        outputDict.update({"postCode":entry["postCode"].split(" ")})
        outputDict.update({"url":entry["url"]})
        outputDict.update({"inhoudInM2":float(entry["Oppervlakten en inhoud"]["Inhoud"].replace(" mÂ³",""))})
        # outputDict.update({"bouwJaar":int(entry["Bouw"]["Bouwjaar"].replace("Voor ",""))})
        outputDict.update({"energieLabel":entry["Energie"]["Energielabel"].replace(" ","")})

        
        outputArray.append(outputDict)
    fl.WriteDataToJSON(outputFile,outputArray)
        

    
parseFundaData(outputpath+"/fundaData.json", outputpath+"/fundaDataPARSED.json")