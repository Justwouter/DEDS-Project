import json
import os
import requests
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'

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

def CBSrewriteID(data):
    for entry in data["value"]:
        if "Key" in entry:
            entry.update({"_id":entry.pop("Key")})
        elif "ID" in entry:
            entry.update({"_id":entry.pop("ID")})
    return data
        
        
    



def main(url, dbName):
    baseurl = CBSgetBaseURL(url)
    dataCode = CBSgetDatasetCode(baseurl)
    saveFolder = outputpath+dataCode+"/"
    index = getData(baseurl)
    
    fl.WriteDataToJSON(saveFolder+"cbs"+dataCode+".json",index)
    
    for item in index["value"]:
        dataName = CBSgetDatasetName(item["url"])
        print("Starting "+dataName)
        if  "DataSet" not in dataName:
            data = CBSrewriteID(getData(item["url"]))
            fl.WriteDataToJSON(saveFolder+dataName+".json", data)
            fl.saveDictListToMongo(dbName,dataName,data["value"])
        else:
            data = CBSrewriteID(getData(CBSreplaceDatasetName(url, dataName)))
            fl.WriteDataToJSON(saveFolder+dataName+".json", data)
            fl.saveDictListToMongo(dbName,dataName,data["value"])
        print("Saved "+ dataName)



CBSlinks = [
    [r"https://opendata.cbs.nl/ODataApi/odata/82235NED/","CBS_WoningVoorraad_Standen-mutaties_82235NED"],
    [r"https://opendata.cbs.nl/ODataApi/odata/83704NED/UntypedDataSet?$filter=((Woningtype eq 'T001100') or (Woningtype eq 'ZW10290') or (Woningtype eq 'ZW10340')) and ((Oppervlakteklasse eq 'T001116') or (Oppervlakteklasse eq 'A041692') or (Oppervlakteklasse eq 'A025407') or (Oppervlakteklasse eq 'A025408') or (Oppervlakteklasse eq 'A025409') or (Oppervlakteklasse eq 'A025410') or (Oppervlakteklasse eq 'A025411') or (Oppervlakteklasse eq 'A025412') or (Oppervlakteklasse eq 'A041691')) and ((RegioS eq 'GM0518'))&$select=Woningtype, Oppervlakteklasse, Perioden, RegioS, BeginstandWoningvoorraad_1",
     "CBS_WoningVoorraad_type-regio-klasse_83704NED"],
    [r"https://opendata.cbs.nl/ODataApi/odata/82900NED/UntypedDataSet?$filter=((StatusVanBewoning+eq+%27T001235%27)+or+(StatusVanBewoning+eq+%27A028725%27)+or+(StatusVanBewoning+eq+%27A028726%27))+and+((RegioS+eq+%27GM0518%27))&$select=StatusVanBewoning,+Perioden,+RegioS,+TotaleWoningvoorraad_1,+Koopwoningen_2,+TotaalHuurwoningen_3,+EigendomWoningcorporatie_4,+EigendomOverigeVerhuurders_5,+EigendomOnbekend_6",
     "CBS_WoningVoorraad_eigendom-bewoning-regio_82900NED"],
    [r"https://opendata.cbs.nl/ODataApi/odata/82550NED/UntypedDataSet?$filter=((Woningtype+eq+%27T001100%27)+or+(Woningtype+eq+%27ZW10290%27)+or+(Woningtype+eq+%27ZW10340%27))+and+((Bouwjaarklasse+eq+%27T001018%27)+or+(Bouwjaarklasse+eq+%27ZW10401%27)+or+(Bouwjaarklasse+eq+%27ZW25794%27)+or+(Bouwjaarklasse+eq+%27ZW10408%27)+or+(Bouwjaarklasse+eq+%27ZW10409%27)+or+(Bouwjaarklasse+eq+%27ZW25796%27)+or+(Bouwjaarklasse+eq+%27ZW25797%27))+and+((RegioS+eq+%27GM0518%27))&$select=Woningtype,+Bouwjaarklasse,+Perioden,+RegioS,+BeginstandWoningvoorraad_1,+GemiddeldeOppervlakte_2",
     "CBS_WoningVoorraad_oppervlakte-type-bouwjaar-regio_82550NED"],
    [r"https://opendata.cbs.nl/ODataApi/odata/83625NED/UntypedDataSet?$filter=((Perioden+eq+%271995JJ00%27)+or+(Perioden+eq+%272000JJ00%27)+or+(Perioden+eq+%272005JJ00%27)+or+(Perioden+eq+%272010JJ00%27)+or+(Perioden+eq+%272015JJ00%27)+or+(Perioden+eq+%272020JJ00%27)+or+(Perioden+eq+%272021JJ00%27)+or+(Perioden+eq+%272022JJ00%27))+and+((RegioS+eq+%27GM0202%27)+or+(RegioS+eq+%27GM0106%27)+or+(RegioS+eq+%27GM0518%27)+or+(RegioS+eq+%27GM0014%27)+or+(RegioS+eq+%27GM0392%27)+or+(RegioS+eq+%27GM0796%27)+or+(RegioS+eq+%27GM0080%27)+or+(RegioS+eq+%27GM0995%27)+or+(RegioS+eq+%27GM0935%27)+or+(RegioS+eq+%27GM0687%27)+or+(RegioS+eq+%27GM0344%27)+or+(RegioS+eq+%27GM0193%27))&$select=Perioden,+RegioS,+GemiddeldeVerkoopprijs_1",
     "CBS_WoningVoorraad_gemprijzen-regio_83625NED"]
]

for item in CBSlinks:
    main(item[0],item[1])

