import json
import os
import requests
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'

# Url voor woningen in DH per jaar per type
urlDHTypeByYear = r"https://opendata.cbs.nl/ODataApi/odata/83704NED/TypedDataSet?$filter=((Woningtype+eq+%27T001100%27)+or+(Woningtype+eq+%27ZW10290%27)+or+(Woningtype+eq+%27ZW10340%27))+and+((Oppervlakteklasse+eq+%27T001116%27))+and+((RegioS+eq+%27GM0518%27))&$select=Woningtype,+Oppervlakteklasse,+Perioden,+RegioS,+BeginstandWoningvoorraad_1"
baseurl = "https://opendata.cbs.nl/ODataApi/odata/83704NED"

def getData(url):
    request = requests.get(url)
    if(request.ok):
        data = json.loads(request.content)
        return data
    return None

def CBSgetDatasetCode(url):
    return url.split("/")[5]
def CBSgetBaseURL(url):
    return "/".join(url.split("/")[:6])+"/"
def CBSgetDatasetName(url):
    return url.split("/")[6].split("?")[0]
def CBSreplaceDatasetName(url, name):
    baseurl = CBSgetBaseURL(url)
    query = "?"+("/".join(url.split("/")[6:]).split("?")[1])
    return baseurl+str(name)+query





def main(url):
    baseurl = CBSgetBaseURL(url)
    saveFolder = outputpath+CBSgetDatasetCode(baseurl)+"/"
    index = getData(baseurl)
    
    fl.WriteDataToJSON(saveFolder+"cbs"+CBSgetDatasetCode(baseurl)+".json",index)
    
    for item in index["value"]:
        dataName = CBSgetDatasetName(item["url"])
        if  "DataSet" not in dataName:
            data = getData(item["url"])
            fl.WriteDataToJSON(saveFolder+dataName+".json", data)
            fl.saveDictListToMongo("CBS_WoningVooraad",dataName,data["value"])
        else:
            data = getData(CBSreplaceDatasetName(url, dataName))
            fl.WriteDataToJSON(saveFolder+dataName+".json", data)
            fl.saveDictListToMongo("CBS_WoningVooraad",dataName,data["value"])
        print("Saved "+ dataName)

main(urlDHTypeByYear)