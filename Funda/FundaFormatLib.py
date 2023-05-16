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
        outputDict.update({"postCode":''.join(entry["postCode"].split(" ")[:2])}) #Postcode split from region
        outputDict.update({"regio":' '.join(entry["postCode"].split(" ")[2:])}) #Regio split from postcode use 4: to remove "Den Haag"
        outputDict.update({"url":entry["url"]}) #Does this need explaining?
        # outputDict.update({"huisNr":str(entry["titel"].split(" ")[-1])}) #Annoyingly there is no standard for listing this resulting in 44-45 or 45A-C
        
        #Lambdas & method formatters
        outputDict.update({"oppervlakteInM2":handlePerceel((lambda x: getKeyOrNone(entry, "Oppervlakten en inhoud.Inhoud") if x is None else x)(getKeyOrNone(entry, "Oppervlakten en inhoud.Perceel")))})
        outputDict.update({"bouwJaar":handleYears(getKeyOrNone(entry,"Bouw.Bouwjaar"))})
        outputDict.update({"type":handleTypes((lambda x: getKeyOrNone(entry,"Bouw.Soort woonhuis") if x is None else x)(getKeyOrNone(entry,"Bouw.Soort appartement")))})
        outputDict.update({"vraagPrijs":handleAskingPrice(getKeyOrNone(entry,"Overdracht.Vraagprijs"))})

        outputDict.update({"energieLabel":entry["Energie"]["Energielabel"].replace(" ","")})
        outputDict.update({"isolatieKenmerk":entry["Energie"]["Isolatie"]})
        
        if outputDict not in outputArray: outputArray.append(outputDict)
    fl.WriteDataToJSON(outputFile,outputArray)
    return outputArray

def removeStupidKeys(inputFile: str, outputFile: str): #Removes dynamically changing keys from the dataset to avoid clutter in the DB. Also adds id's for good measure
    with open(inputFile, encoding="utf8") as input:
        input = json.load(input)
    for entry in input:
        entry.pop("Kadastrale gegevens", None)
        entry.update({"_id":int(entry["url"].split("/")[5].split("-")[1])})
        
    fl.WriteDataToJSON(outputFile,input)
    return input



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
        return str(value.replace(" m²","").replace(" m³","").replace(",","").split(" ")[0])
    return None
def handleAskingPrice(value):
    if value is not None:
        return int(value.replace("€ ","").split(" ")[0].replace('.', ''))
    return None
    
    
parseFundaData(outputpath+"/fundaData.json", outputpath+"/fundaDataPARSED.json")
fl.saveDictListToMongo("FundaDB","FundaDataParsed",parseFundaData(outputpath+"/fundaData.json", outputpath+"/fundaDataPARSED.json"))

removeStupidKeys(outputpath+"/fundaData.json", outputpath+"/fundaDataCleaned.json")
# with open(outputpath+"/fundaDataCleaned.json", encoding="utf8") as input:
#     squashedList = []
#     data = json.load(input)
#     for entry in data:
#         squashedList.append(fl.squashDict(entry))
#     fl.saveDictListToMongo("FundaDB","FundaDataRaw",squashedList)
