import json
import os
import requests
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'

# Url voor woningen in DH per jaar per type
urlDHTypeByYear = r"https://opendata.cbs.nl/ODataApi/odata/82235NED/"
# https://opendata.cbs.nl/#/CBS/nl/dataset/83704NED/table?searchKeywords=voorraad%20woningen <-- Use this to generate query strings
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
    try:
        query = "?"+("/".join(url.split("/")[6:]).split("?")[1])
    except:
        query = ""
    return baseurl+str(name)+query





def main(url):
    baseurl = CBSgetBaseURL(url)
    dataCode = CBSgetDatasetCode(baseurl)
    saveFolder = outputpath+dataCode+"/"
    index = getData(baseurl)
    
    fl.WriteDataToJSON(saveFolder+"cbs"+dataCode+".json",index)
    
    for item in index["value"]:
        dataName = CBSgetDatasetName(item["url"])
        print("Starting "+dataName)
        if  "DataSet" not in dataName:
            data = getData(item["url"])
            fl.WriteDataToJSON(saveFolder+dataName+".json", data)
            fl.saveDictListToMongo("CBS_"+dataCode,dataName,data["value"])
        else:
            data = getData(CBSreplaceDatasetName(url, dataName))
            fl.WriteDataToJSON(saveFolder+dataName+".json", data)
            fl.saveDictListToMongo("CBS_"+dataCode,dataName,data["value"])
        print("Saved "+ dataName)

main(urlDHTypeByYear)