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
        outputDict.update({"postCode":entry["postCode"]})
        outputDict.update({"url":entry["url"]})
        outputArray.append(outputDict)
    fl.WriteDataToJSON(outputFile,outputArray)
    fl.WriteDataToCSV(outputFile,outputpath+"/")
        
    
    
parseFundaData(outputpath+"/fundaData.json", outputpath+"/fundaDataPARSED.json")