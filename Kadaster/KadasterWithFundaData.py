import json
import os
import KadasterData
import KadasterDataFormatLib
import FileLib as fl


inputpath = os.path.dirname(__file__)+'/input/'
outputpath = os.path.dirname(__file__)+'/output/'

def FKDRemapKeys(data):
    for entry in data:
        if "id" in entry:
            entry.update({"BAGid": entry.pop("id")})
    return data


def combineFundaAndKadaster(inputFile):
    with open(inputFile, "r", encoding="utf-8") as inFile:
        fundaData = json.load(inFile)
        output = []
        for entry in fundaData:
            kadasterDataRaw = KadasterData.KDgetDataByPostCode(entry["postCode"])
            kadasterDataRaw = kadasterDataRaw["response"]["docs"]
            for huis in kadasterDataRaw:
                if (huis["type"] == "adres"):
                    print(huis["straatnaam"])
                    kData = KadasterDataFormatLib.KDParseData(huis)
                    entry.update(kData)
                    output.append(entry)
                    break
    return FKDRemapKeys(output)
            
data = combineFundaAndKadaster(inputpath+"fundaDataPARSED.json")
fl.WriteDataToJSON(outputpath+"kadasterDataFUNDA.json",data)
fl.saveDictListToMongo("FundaKadasterDB","fundaWithKadaster",data)