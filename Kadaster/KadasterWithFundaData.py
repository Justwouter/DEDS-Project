import json
import os
import Kadasterdata


inputpath = os.path.dirname(__file__)+'/input/'


def combineFundaAndKadaster(inputFile):
    with open(inputFile, "r") as inFile:
        fundaData = json.load(inFile)
        for entry in fundaData:
            kadasterDataRaw = Kadasterdata.KDgetDataByPostCode(entry["postCode"])
            for entry in kadasterDataRaw:
                if (entry["type"] == "adres"):
                    print(entry)
                    break
            
combineFundaAndKadaster(inputpath+"fundaDataParsed.json")