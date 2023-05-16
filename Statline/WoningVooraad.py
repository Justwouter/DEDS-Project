import json
import os
import requests
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'

# Url voor woningen in DH per jaar per type
urlDHTypeByYear = r"https://opendata.cbs.nl/ODataApi/odata/83704NED/UntypedDataSet?$filter=((Woningtype eq 'T001100') or (Woningtype eq 'ZW10290') or (Woningtype eq 'ZW10340')) and ((Oppervlakteklasse eq 'T001116') or (Oppervlakteklasse eq 'A041692') or (Oppervlakteklasse eq 'A025407') or (Oppervlakteklasse eq 'A025408') or (Oppervlakteklasse eq 'A025409') or (Oppervlakteklasse eq 'A025410') or (Oppervlakteklasse eq 'A025411') or (Oppervlakteklasse eq 'A025412') or (Oppervlakteklasse eq 'A041691')) and ((RegioS eq 'GM0518'))&$select=Woningtype, Oppervlakteklasse, Perioden, RegioS, BeginstandWoningvoorraad_1"
baseurl = "https://opendata.cbs.nl/ODataApi/odata/83704NED"
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
    query = "?"+("/".join(url.split("/")[6:]).split("?")[1])
    return baseurl+str(name)+query





def main(url):
    baseurl = CBSgetBaseURL(url)
    saveFolder = outputpath+CBSgetDatasetCode(baseurl)+"/"
    index = getData(baseurl)
    
    fl.WriteDataToJSON(saveFolder+"cbs"+CBSgetDatasetCode(baseurl)+".json",index)
    
    for item in index["value"]:
        dataName = CBSgetDatasetName(item["url"])
        print("Starting "+dataName)
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