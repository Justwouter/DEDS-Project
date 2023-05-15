import os
from threading import Thread, Lock
import time
import warnings
from selenium import webdriver
from selenium.webdriver.firefox.options import Options 
from selenium.webdriver.common.by import By
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import undetected_chromedriver as uc
import FileLib as fl

url = "https://funda.nl"
outputpath = os.path.dirname(__file__)+'/output/'
pages = []
adLinks = []
listings = []
wait = ""

        
def getBrowser():
    webdriver.Chrome.get
    startTime = time.time()
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    options = webdriver.ChromeOptions()
    # options.add_argument("--log-level=3")
    # options.add_experimental_option('excludeSwitches', ['enable-logging'])
    #options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("enable-automation")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "normal"
    return uc.Chrome(desired_capabilities=caps, chrome_options=options)    

def FundaRefuseCookie(browser: webdriver.Chrome, link:str):
    browser.get(link)
    try:
        wait.until(lambda Waitlol: browser.find_element(By.ID, 'onetrust-accept-btn-handler'))
        browser.find_element(By.ID, "onetrust-accept-btn-handler").click()
    except:
        return 0
         
def FundaGetLisitings(browser: webdriver.Chrome, link):
    FundaRefuseCookie(browser,link)
    Advertenties = browser.find_elements(By.CLASS_NAME, 'search-result__header-title-col')
    print(len(Advertenties))
    adLinks = []
    for element in Advertenties:
        adLinks.append(element.find_elements(By.TAG_NAME, "a")[1].get_attribute("href"))
    return adLinks
    
def FundaGetSearchURL(browser: webdriver.Chrome,searchterm):
    FundaRefuseCookie(browser, url)
    browser.find_element(By.ID, "autocomplete-input").send_keys(searchterm)
    browser.find_element(By.XPATH, "//button[@class = 'fd-btn fd-btn--block fd-btn--primary-alt']").click() #Click search button
    return browser.current_url

def FundaGetPageAmount(browser: webdriver.Chrome):
    pageBar = browser.find_element(By.XPATH , "//nav[@class = 'pagination']")
    pageAmount = int(pageBar.find_elements(By.TAG_NAME, "a")[-2].get_attribute("data-pagination-page"))
    return pageAmount
        
        
        
def FundaGetListingInfo(browser: webdriver.Chrome, link):
    FundaRefuseCookie(browser,link)
    titel = browser.find_element(By.XPATH, "//span[@class = 'object-header__title']").text
    postCode = browser.find_element(By.XPATH, "//span[@class = 'object-header__subtitle fd-color-dark-3']").text
    realtor = browser.find_element(By.XPATH, "//a[@class = 'object-contact-aanbieder-link']").text
    kenmerken = {"titel":titel, "realtor":realtor,"postCode":postCode,"url": link}
    
    kenmerkenWindow = browser.find_element(By.XPATH, "//div[@class = 'object-kenmerken-body']")
    kenmerkHeaders = kenmerkenWindow.find_elements(By.XPATH, "//h3[@class = 'object-kenmerken-list-header']")
    kenmerkenLists = kenmerkenWindow.find_elements(By.XPATH, "//dl[@class = 'object-kenmerken-list']")
    
    for i in range(len(kenmerkHeaders)):
        element = kenmerkenLists[i]
        kenmerkTags = element.find_elements(By.TAG_NAME, "dt")
        kenmerkValues = element.find_elements(By.TAG_NAME, "dd")
        output = {}
        valueSkip = 0
        j = 0
        while j < len(kenmerkTags):
            tag = kenmerkTags[j]
            value = kenmerkValues[j+valueSkip]

            if("object-kenmerken-group-header" in tag.get_attribute("class")): # Handle the damned additional lists like "Gebruiksoppervlakten" 
                valueSkip +=1
                indentGroup = kenmerkValues[j+valueSkip]
                
                indentTags = indentGroup.find_elements(By.TAG_NAME, "dt")
                indentValues = indentGroup.find_elements(By.TAG_NAME, "dd")
                
                indentDict = {}
                for count in range(len(indentTags)):
                    j += 1
                    iTag = indentTags[count]
                    iValue = indentValues[count]
                    indentDict.update({iTag.text:iValue.text})
                output.update({tag.text:indentDict})
            else:
                output.update({tag.text:value.text})
            j +=1
        kenmerken.update({kenmerkHeaders[i].text:output})
    return kenmerken
    
    
    
def getItemFromLock(lock: Lock, list):
    lock.acquire()
    data = list.pop()
    lock.release()
    return data
    
lock = Lock()
dataLock = Lock()
fundaDataLock = Lock()



class MyThread(Thread):
    
    def __init__(self, name):
        Thread.__init__(self)
        self.name = name

    def run(self):
        threadName = self.name
        browser = getBrowser()

        while len(pages) > 0:
            pageNr = getItemFromLock(lock, pages)
            data = FundaGetLisitings(browser,baseURL+"/p"+str(pageNr))
            
            dataLock.acquire()
            adLinks.extend(data)
            dataLock.release()
            
        while len(adLinks) > 0:
            adURL = getItemFromLock(dataLock,adLinks)
            try:
                listingInfo = FundaGetListingInfo(browser, adURL)
            except Exception as e:
                print(e)
                browser.close()
                browser = getBrowser()
                adLinks.append(adURL)
                continue
            
            
            fundaDataLock.acquire()
            listings.append(listingInfo)
            fl.WriteDataToJSON(outputpath+r"\fundaData.json",listings)
            fundaDataLock.release()
            
        browser.close()

def create_threads():
    start_time = time.time()
    for i in range(int(len(pages)/50)+1): #Spawns a thread for each entry in a list threads (len(urls)) plus one if the list is empty
        name = "Thread #%s" % (i)
        my_thread = MyThread(name)
        my_thread.start()
    my_thread.join() 
    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution time:", execution_time, "seconds")
    time.sleep(5)
    
if __name__ == "__main__":
    browser = getBrowser()
    wait = ui.WebDriverWait(browser, 5)
    #Testing
    # fl.WriteDataToJSON(r"D:\Coding\SE2\DEDS\DEDS-Project\fundaData2.json",FundaGetListingInfo(browser, "https://www.funda.nl/koop/den-haag/huis-42101784-plesmanlaan-24/"))
    
    baseURL = FundaGetSearchURL(browser, "Den Haag")
    pageAmount = FundaGetPageAmount(browser)
    browser.close()
    for i in range(pageAmount):
        pages.append(i)
    create_threads()