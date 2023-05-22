import os,json
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'
outputDict = {}
attributes = ["id","huisletter","huisnummer","postcode","straatnaam","woonplaatsnaam","buurtnaam","wijknaam","gemeentenaam","provincienaam",]

def KDRemapID(data):
    for entry in data:
        if "id" in entry:
            entry.update({"_id":entry.pop("id")})
    return data



with open(outputpath+"kadasterData.json","r") as inputFile:
    data = json.load(inputFile)
    for postCode in data:
        formatList = []
        for huis in data[postCode]:
            if(huis["type"] == "adres"):
                formatDict = {}
                for attrib in attributes:
                    formatDict[attrib] = fl.getKeyOrNone(huis,attrib)
                formatList.append(formatDict)
            outputDict[postCode] = KDRemapID(formatList)
        print("Finished postCode " + postCode + " " + data.keys().index(postCode)+ " out of "+ len(data.keys()))
        fl.saveDictListToMongo("KadasterAdresDB",postCode,formatList)   


fl.WriteDataToJSON(outputpath+"kadasterDataPARSED.json",outputDict)
         
