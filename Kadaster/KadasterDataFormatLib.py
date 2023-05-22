import os,json
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'
outputDict = {}
attributes = ["huisletter","huisnummer","postcode","straatnaam","woonplaatsnaam","buurtnaam","wijknaam","gemeentenaam","provincienaam",]






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
            outputDict[postCode] = formatList



fl.WriteDataToJSON(outputpath+"kadasterDataPARSED.json",outputDict)
                        
            
