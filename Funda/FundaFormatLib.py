import json
import os
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'

def parseFundaData(inputFile: str, outputFile: str):
    with open(inputFile, encoding="utf8") as input:
        inputDict = json.load(input)
    outputArray = []
    for entry in inputDict:
        outputDict = {}
        
        outputDict.update({"_id":int(entry["url"].split("/")[5].split("-")[1])}) #Parse the unique number from the URL and use it as id for mongo
        outputDict.update({"adres":entry["titel"]}) #Listing adress
        # outputDict.update({"huisNr":str(entry["titel"].split(" ")[-1])}) #Annoyingly there is no standard for listing this resulting in 44-45 or 45A-C
        outputDict.update({"postCode":''.join(entry["postCode"].split(" ")[:2])}) #Postcode split from region
        outputDict.update({"regio":' '.join(entry["postCode"].split(" ")[2:])}) #Regio split from postcode use 4: to remove "Den Haag"
        outputDict.update({"url":entry["url"]}) #Does this need explaining?
        
        outputDict.update({"oppervlakteInM2":handlePerceel((lambda x: getKeyOrNone(entry, "Oppervlakten en inhoud.Inhoud") if x is None else x)(getKeyOrNone(entry, "Oppervlakten en inhoud.Perceel")))})
        outputDict.update({"bouwJaar":handleYears(getKeyOrNone(entry,"Bouw.Bouwjaar"))})
        outputDict.update({"type":handleTypes((lambda x: getKeyOrNone(entry,"Bouw.Soort woonhuis") if x is None else x)(getKeyOrNone(entry,"Bouw.Soort appartement")))})
                
        outputDict.update({"energieLabel":entry["Energie"]["Energielabel"].replace(" ","")})
        outputDict.update({"isolatieKenmerk":entry["Energie"]["Isolatie"]})
        
        if outputDict not in outputArray: outputArray.append(outputDict)
    fl.WriteDataToJSON(outputFile,outputArray)
    return outputArray
   
def getKeyOrNone(dictionary, key):
    keys = key.split(".")
    value = dictionary
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return None
    return value
    
def handleYears(value):
    if value is not None:
        return int(value.replace("Voor ",""))
    else:
        return 0
def handlePerceel(value):
    if value is not None:
        return float(value.replace(" m²","").replace(" m³",""))
    return 0.0
def handleTypes(value):
    if value is not None:
        return str(value.replace(" m²","").replace(" m³","").split(" ")[0])
    return None
    
fl.saveDictToMongo("FundaDB","FundaDataParsed",parseFundaData(outputpath+"/fundaData.json", outputpath+"/fundaDataPARSED.json"))

with open(outputpath+"/fundaData.json", encoding="utf8") as input:
    fl.saveDictToMongo("FundaDB","FundaDataRaw",fl.squashDict(json.load(input)))
