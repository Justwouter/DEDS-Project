import os,json,requests
import FileLib as fl

outputpath = os.path.dirname(__file__)+'/output/'
inputpath = os.path.dirname(__file__)+'/input/'


def KDgetDataByPostCode(postCode):
    url = "https://geodata.nationaalgeoregister.nl/locatieserver/v3/free?q="+postCode+"&fl=*&fq=*:*&rows=10&start=0"
    request = requests.get(url)
    if(request.ok):
        data = json.loads(request.content)
        return data
    return None


# data = KDgetDataByPostCode("2498cg")["response"]["docs"]
# fl.WriteDataToJSON(outputpath+"kadasterData.json",data)
def KDgetData():
    output = {}
    with open(inputpath+"data.json","r") as inputFile:
        inputData = json.load(inputFile)
        for huis in inputData:
            postcode = huis["postCode"]
            print(postcode)
            outData = KDgetDataByPostCode(postcode)["response"]["docs"]
            output[postcode] = outData

    fl.WriteDataToJSON(outputpath+"kadasterData.json",output)
        
