import csv
import os
from threading import Thread, Lock
import time
from csv import writer
import warnings
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options 
from selenium.webdriver.common.by import By
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import undetected_chromedriver as uc
import FileLib as fl

url = "https://funda.nl"
outputpath = os.path.dirname(__file__)+'/output/'
outputfile = os.path.dirname(__file__)+'/output/bol.txt'
pages = []
adLinks = []
wait = ""



def writeToOutput(item):
    with open(outputfile, 'a', encoding="utf8", newline="") as out:
        out.write(item+"\n")
        
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

def FundaRefuseCookie(browser: webdriver.Chrome, link):
    browser.get(link)
    try:
        wait.until(lambda Waitlol: browser.find_element(By.ID, 'onetrust-accept-btn-handler'))
        browser.find_element(By.ID, "onetrust-accept-btn-handler").click()
    except:
        return 0
      
        
def FundaGetInfo(browser: webdriver.Chrome, link):
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
    print(pageAmount)
    return pageAmount
        
    
    
lock = Lock()
fileLock = Lock()
class MyThread(Thread):
    
    def __init__(self, name):
        Thread.__init__(self)
        self.name = name

    def run(self):
        threadName = self.name
        browser = getBrowser()

        while len(pages) > 0:
            
            lock.acquire()
            pageNr = pages.pop()
            lock.release()
            data = FundaGetInfo(browser,baseURL+"/p"+str(pageNr))
            fileLock.acquire()
            adLinks.append(data)
            fl.WriteDataToJSON(r"D:\Coding\SE2\DEDS\DEDS-Project\data.json",adLinks)
            fileLock.release()
            
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
    baseURL = FundaGetSearchURL(browser, "Den Haag")
    pageAmount = FundaGetPageAmount(browser)
    browser.close()
    for i in range(pageAmount):
        pages.append(i)
    create_threads()