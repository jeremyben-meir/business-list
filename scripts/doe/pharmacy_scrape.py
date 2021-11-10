from scripts.common import DirectoryFields
import pickle
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import csv
import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from lxml import etree
from datetime import date

class PharmacyScraper():
    def __init__(self):
        self.df = pd.DataFrame(columns=['Type','Legal Name','Trade Name','Registration No','Date First Registered','Registration Begins','Registered through','Establishment Status','Successor','Address'])
        self.countylist =['//*[@id="content_column"]/div[4]/form/div[4]/select/option[42]','//*[@id="content_column"]/div[4]/form/div[4]/select/option[4]','//*[@id="content_column"]/div[4]/form/div[4]/select/option[25]','//*[@id="content_column"]/div[4]/form/div[4]/select/option[32]','//*[@id="content_column"]/div[4]/form/div[4]/select/option[44]']
        self.charlist = 'abcdefghijklmnopqrstuvwxy1234567890!@#$%^&*()'
        self.url = "http://www.op.nysed.gov/opsearches#"
        self.department = "doe"

    def start_scrape(self):
        self.driver = webdriver.Chrome(DirectoryFields.LOCAL_WEBDRIVER_PATH)
        for i in self.charlist:
            for y in self.countylist:
                        
                self.driver.get(self.url)
                select = Select(self.driver.find_element_by_xpath('//*[@id="content_column"]/div[4]/form/div[1]/select'))
                select.select_by_index(0)
                name=self.driver.find_element_by_xpath('//*[@id="content_column"]/div[4]/form/div[3]/input')
                name.send_keys(i)
                self.driver.find_element_by_xpath('//*[@id="content_column"]/div[4]/form/div[5]/select/option[3]').click()

                self.driver.find_element_by_xpath(y).click()
                self.driver.find_element_by_xpath('//*[@id="content_column"]/div[4]/form/div[6]/input[1]').click()
                WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content_column"]/h1')))
                isPresent = 1
                while isPresent > 0:
                    isPresent = len(self.driver.find_elements_by_xpath('/html/body/div/div[3]/div[2]/div[2]/form/input[7]'))
                    try:
                        first = self.driver.find_element_by_xpath('//*[@id="content_column"]/a[1]').text
                        for j in range(1,17):
                            self.driver.find_element_by_xpath(f"/html/body/div/div[3]/div[2]/div[2]/a[{str(j)}]").click()
                            
                            WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content_column"]')))
                            given_text = self.driver.find_element_by_xpath('//*[@id="content_column"]').text
                            given_text = given_text.split('\n')

                            try:
                                type_ = [e.replace('Type : ','') for e in given_text if 'Type' in e][0]
                            except:
                                type_ = None
                            
                            try:
                                l_name = [e.replace('Legal Name : ','') for e in given_text if 'Legal Name' in e][0]
                            except:
                                l_name = None
                            
                            try:
                                t_name = [e.replace('Trade Name :','').replace('Trade Name : ','') for e in given_text if 'Trade Name' in e][0]
                            except:
                                t_name = None
                            
                            try: 
                                index_street = [i for i, s in enumerate(given_text) if 'Street Address' in s][0]
                                index_reg = [i for i, s in enumerate(given_text) if 'Registration No' in s][0]
                                ad = ''
                                for q in range(index_street+1,index_reg):
                                    new = '\n'+given_text[q]
                                    ad += new

                            except:
                                ad = None
                            
                            try:
                                reg_no = [e.replace('Registration No : ','') for e in given_text if 'Registration No' in e][0]
                            except:
                                reg_no = None

                            try:
                                first_reg = [e.replace('Date First Registered : ','') for e in given_text if 'Date First Registered' in e][0]
                            except:
                                first_reg = None
                            
                            try:
                                reg_beg = [e.replace('Registration Begins : ','') for e in given_text if 'Registration Begins' in e][0]
                            except:
                                reg_beg = None
                            
                            try:
                                reg_thru = [e.replace('Registered through : ','') for e in given_text if 'Registered through' in e][0]
                            except:
                                reg_thru = None
                            
                            try:
                                es = [e.replace('Establishment Status : ','') for e in given_text if 'Establishment Status' in e][0]
                            except:
                                es = None
                            
                            try:
                                suc = [e.replace('Successor : ','') for e in given_text if 'Successor' in e][0]
                            except:
                                suc = None
                            
                            row = [type_, l_name, t_name, reg_no, first_reg, reg_beg, reg_thru, es, suc, ad]
                            row = pd.Series(row, index = self.df.columns)
                            self.df = self.df.append(row, ignore_index=True)
                            
                            self.driver.back()

                        self.driver.find_element_by_xpath('/html/body/div/div[3]/div[2]/div[2]/form/input[7]').click()
                        
                        claim = True
                        while claim == True:
                            try:
                                if self.driver.find_element_by_xpath('//*[@id="content_column"]/a[1]').text == first:
                                    print(self.driver.find_element_by_xpath('//*[@id="content_column"]/a[1]').text)
                                    time.sleep(1)
                                else:
                                    claim = False
                            except:
                                if self.driver.find_element_by_xpath('//*[@id="content_column"]/a[1]').text == first:
                                    print(self.driver.find_element_by_xpath('//*[@id="content_column"]/a[1]').text)
                                    time.sleep(1)
                                else:
                                    claim = False                            

                    except:
                        pass
                    
        self.driver.close()
        self.df.to_csv(f"{DirectoryFields.S3_PATH}data/doe/pharmacy/{self.department}_pharmacy_{date.today()}_scrape.csv", index=False, quoting=csv.QUOTE_ALL)
        # self.df.to_csv(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doe/pharmacy/{self.department}_pharmacy_{date.today()}_scrape.csv", index=False, quoting=csv.QUOTE_ALL)

if __name__ == '__main__':
    scraper = PharmacyScraper()
    scraper.start_scrape()
