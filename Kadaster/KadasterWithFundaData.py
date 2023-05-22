import json
import os
import Kadasterdata


inputpath = os.path.dirname(__file__)+'/input/'


def combineFundaAndKadaster(inputFile):
    with open(inputFile, "r") as inFile:
        fundaData = json.load(inFile)
        for entry in fundaData:
            kadasterDataRaw = Kadasterdata.KDgetDataByPostCode(entry["postCode"])
            for postcode in kadasterDataRaw:
                for huis in kadasterDataRaw[postcode]:
                    if (huis["type"] == "adres"):
                        print(huis)
                        break
            
combineFundaAndKadaster(inputpath+"fundaDataParsed.json")