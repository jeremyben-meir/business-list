from selenium import webdriver
import time
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import Select
import os
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import urllib.request
from time import process_time
from captcha_breaker import solve_captcha
import threading
from global_vars import *

waiter = []

def makeAction():
    userInput = input("ACTION: ")
    if userInput == 'stop':
        machine.inAction = False
        for x in machine.setOfDrivers:
            x.continueOn = False
    elif userInput == 'more':
        machine.iteratorLimit+=1
        makeAction()
    else:
        makeAction()

class driver_thread():
    zipcodeToSearch = ""
    continueOn = True
    completed = False
    valsThatHaveBeenAccessed = []
    gottenvals = open(LOCAL_LOCUS_PATH + "data_scrape/reference_usa/gotten_pages.txt", 'a')

    def elementExistAtClass(self, class_name):
        
        try:
            self.driver.find_element_by_class_name(class_name)
            return True
        except:
            return False

    def elementExistAtId(self, id_name):
        try:
            self.driver.find_element_by_id(id_name)
            return True
        except:
            return False
    
    def waitForDownloadIcon(self, timeout):
        if timeout:
            start_time = process_time()
            while not self.elementExistAtClass("originLoading") and process_time()-start_time < 10:
                pass
        else:
            while not self.elementExistAtClass("originLoading"):
                pass

    def waitForPageToStopLoading(self, timeout):
        if timeout:
            start_time = process_time()
            while self.elementExistAtClass("originLoading") and process_time()-start_time < 10:
                pass
        else:
            while self.elementExistAtClass("originLoading"):
                pass
        self.isThisACaptchaPage()
    
    def waitForCheckboxPage(self, timeout):
        if timeout:
            start_time = process_time()
            while not self.elementExistAtId("checkboxCol") and process_time()-start_time < 20:
                pass
        else:
            while not self.elementExistAtId("checkboxCol"):
                pass
    
    def waitForDownloadPageToLoad(self):
        start_time = process_time()
        while not self.elementExistAtId("detailDetail") and process_time()-start_time < 20:
            pass
    
    def already_retrieved_vals(self):
        readgottenvals = open(LOCAL_LOCUS_PATH + "data_scrape/reference_usa/gotten_pages.txt", 'r')
        valsAccessed = []
        for line in readgottenvals:
            valsAccessed.append(line.rstrip('\n'))
        readgottenvals.close()
        return valsAccessed

    def restart_thread(self):
        self.driver.close()
        if self.continueOn:
            self.driver = webdriver.Chrome(LOCAL_WEBDRIVER_PATH)
            self.valsThatHaveBeenAccessed = self.already_retrieved_vals()
            self.run()

    def isThisACaptchaPage(self):
        start_time = process_time()
        while not self.elementExistAtId("captchaValidationMessage") and process_time()-start_time < 4:
            pass
        while self.elementExistAtId("captchaValidationMessage"):
            img = self.driver.find_element_by_xpath("//*[@id='captcha-image']")
            src = img.get_attribute('src')
            try:
                urllib.request.urlretrieve(src, LOCAL_LOCUS_PATH + "captcha_breaker/"+str(self.zipcodeToSearch)+"current_captcha.png")
            except:
                time.sleep(2)
                img = self.driver.find_element_by_xpath("//*[@id='captcha-image']")
                src = img.get_attribute('src')
                urllib.request.urlretrieve(src, LOCAL_LOCUS_PATH + "captcha_breaker/"+str(self.zipcodeToSearch)+"current_captcha.png")
            time.sleep(1)
            resultString = solve_captcha.solve(LOCAL_LOCUS_PATH + "captcha_breaker/"+str(self.zipcodeToSearch)+"current_captcha.png")
            try:
                if resultString != "":
                    # print(resultString[:4])
                    twoAddingNums = resultString[:4].strip("?").split("+")
                    result = int(twoAddingNums[0]) + int(twoAddingNums[1])
                    inputBox = self.driver.find_element_by_xpath("//*[@id='Attempt']")
                    inputBox.clear()
                    inputBox.send_keys(str(result))
                    time.sleep(2)
                    try:
                        self.driver.find_element_by_xpath("//*[@id='captchaValidation']/a[2]").click()
                    except:
                        self.driver.find_element_by_xpath(" //*[@id='captchaValidation']/a").click()
            except:
                pass
            self.driver.find_element_by_xpath("//*[@id='captcha-refresh']").click()
            time.sleep(1)
    
    def run(self):
        try:
            self.driver.get('http://www.referenceusa.com.proxy.library.cornell.edu/UsHistoricalBusiness/Search/Custom/')

            user_box = self.driver.find_element_by_xpath("//*[@id='netid']")
            user_box.send_keys("jsb459")
            pass_box = self.driver.find_element_by_xpath("//*[@id='password']")
            pass_box.send_keys("1595Lock")
            # self.driver.find_element_by_xpath("//*[@id='fm1']/div[2]/input[4]").click()
            self.driver.find_element_by_xpath("//*[@id='content']/form/fieldset/div[3]/div/input").click()

            time.sleep(2)

            self.driver.find_element_by_xpath("//*[@id='cs-ZipCode']").click()
            time.sleep(2)
            zip_box = self.driver.find_element_by_xpath("//*[@id='inputGrid1']")
            zip_box.send_keys(str(self.zipcodeToSearch))

            # self.driver.find_element_by_xpath("//*[@id='toggleButton']").click()
            
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[10]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[11]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[12]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[13]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[14]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[15]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[16]").click()
            self.driver.find_element_by_xpath("//*[@id='availableHistoricalYears']/ul/li[17]").click()

            self.driver.find_element_by_xpath("//*[@id='dbSelector']/div/div[2]/div[1]/div[3]/div/a[1]").click()

            self.waitForDownloadIcon(timeout=True)
            self.waitForPageToStopLoading(timeout=True)

            onPage = 1 #int(self.driver.find_element_by_class_name("data-page-number").text.replace(",",""))
            endPage = int(self.driver.find_element_by_class_name("data-page-max").text.replace(",",""))

            while onPage<=endPage and self.continueOn:
                curString = self.zipcodeToSearch + str(onPage)
                if curString not in self.valsThatHaveBeenAccessed:
                    self.driver.find_element_by_xpath("//*[@id='searchResults']/div[1]/div/div[1]/div[2]/div[2]").click()
                    actions = ActionChains(self.driver)
                    actions.send_keys(str(onPage) + Keys.RETURN)
                    actions.perform()

                    self.waitForDownloadIcon(timeout=True)
                    self.waitForPageToStopLoading(timeout=True)
                    self.waitForCheckboxPage(timeout=True)

                    try:
                        self.driver.find_element_by_xpath("//*[@id='checkboxCol']").click()
                    except:
                        self.waitForPageToStopLoading(timeout=True)
                        self.waitForCheckboxPage(timeout=True)
                        self.driver.find_element_by_xpath("//*[@id='checkboxCol']").click()

                    self.driver.find_element_by_xpath("//*[@id='searchResults']/div[1]/div/ul/li[5]/a").click()
                    self.waitForPageToStopLoading(timeout=True)
                    self.waitForDownloadPageToLoad()
                    try:
                        self.driver.find_element_by_xpath("//*[@id='detailDetail']").click()
                    except:
                        self.waitForPageToStopLoading(timeout=True)
                        self.waitForDownloadPageToLoad()
                        self.driver.find_element_by_xpath("//*[@id='detailDetail']").click()

                    global waiter
                    waiter.append(self.zipcodeToSearch)
                    while waiter[0] != self.zipcodeToSearch:
                        pass
                    self.driver.find_element_by_xpath("//*[@id='downloadForm']/div[2]/a[1]").click()
                    self.gottenvals.write(curString + ('\n'))
                    self.gottenvals.flush()
                    time.sleep(1)
                    waiter.pop(0)
                    self.driver.find_element_by_xpath("//*[@id='dbSelector']/div/div[2]/div[1]/ul/li[1]/a").click()
                    self.waitForCheckboxPage(timeout=True)
                    self.driver.find_element_by_xpath("//*[@id='checkboxCol']").click()
                
                onPage+=1
            self.driver.close()
            if onPage>endPage:
                self.completed = True
        except:
            self.restart_thread()
    
    def __init__(self, zipcodeToSearch):
        self.valsThatHaveBeenAccessed = self.already_retrieved_vals()
        self.zipcodeToSearch = zipcodeToSearch
        self.driver = webdriver.Chrome(LOCAL_WEBDRIVER_PATH)


class thread_handler():
    inAction = True
    iterator = 0
    iteratorLimit = 0   
    setOfDrivers = []
    #manhattan_zipcodes = [10026, 10027, 10030, 10037, 10039, 10001, 10011, 10018, 10019, 10020, 10036, 10029, 10035, 10010, 10016, 10017, 10022, 10012, 10013, 10014, 10004, 10005, 10006, 10007, 10038, 10280, 10002, 10003, 10009, 10021, 10028, 10044, 10065, 10075, 10128, 10023, 10024, 10025, 10031, 10032, 10033, 10034, 10040]
    manhattan_zipcodes = [10453, 10457, 10460, 10458, 10467, 10468, 10451, 10452, 10456, 10454, 10455, 10459, 10474, 10463, 10471, 10466, 10469, 10470, 10475, 10461, 10462, 10464, 10465, 10472, 10473, 11212, 11213, 11216, 11233, 11238,11209, 11214, 11228, 11204, 11218, 11219, 11230, 11234, 11236, 11239, 11223, 11224, 11229, 11235, 11201, 11205, 11215, 11217, 11231, 11203, 11210, 11225, 11226, 11207, 11208, 11211, 11222, 11220, 11232, 11206, 11221, 11237, 11361, 11362, 11363, 11364, 11354, 11355, 11356, 11357, 11358, 11359, 11360, 11365, 11366, 11367, 11412, 11423, 11432, 11433, 11434, 11435, 11436, 11101, 11102, 11103, 11104, 11105, 11106, 11374, 11375, 11379, 11385, 11691, 11692, 11693, 11694, 11695, 11697, 11004, 11005, 11411, 11413, 11422, 11426, 11427, 11428, 11429, 11414, 11415, 11416, 11417, 11418, 11419, 11420, 11421, 11368, 11369, 11370, 11372, 11373, 11377, 11378, 10302, 10303, 10310, 10306, 10307, 10308, 10309, 10312, 10301, 10304, 10305, 10314]


    def run_num_of_threads(self):
        for x in self.manhattan_zipcodes:
            if self.inAction:
                self.setOfDrivers.append(driver_thread(str(x)))
                threading.Thread(target=self.setOfDrivers[self.iterator].run).start()
                self.iterator+=1
                while self.inAction and self.iterator >= self.iteratorLimit:
                    for y in self.setOfDrivers:
                        if y.completed and self.inAction:
                            self.iteratorLimit += 1
                            y.completed = False
    
    def __init__(self, iteratorLimit):
        self.iteratorLimit = iteratorLimit
        
    
machine = thread_handler(8)
threading.Thread(target=machine.run_num_of_threads).start()
makeAction()