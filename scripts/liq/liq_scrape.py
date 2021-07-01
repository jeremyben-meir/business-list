from scripts.common import DirectoryFields
import pickle
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
import csv
import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from lxml import etree

class LiquorScrape():
    def __init__(self):
        self.county_list = ['QUEENS','RICHMOND','KINGS','BRONX','NEW YORK']
        self.df = pd.DataFrame(columns=["Premises Name","Trade Name","Zone","Address","County","Serial Number","License Type","License Status","Credit Group","Filing Date","Effective Date","Expiration Date","Principal's Name","URL"])#,"Serial 2","License Class"])
        try:
            # self.df = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "rb"))
            self.links = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "rb"))
            self.start_clicks = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/var-liqour-0", "rb"))
        except:
            self.links = []
            self.start_clicks = (0,2)
    
    async def fetch(self, session, url):
        row = pd.Series(index=self.df.columns, dtype = 'object')
        row.loc["URL"] = url
        counter = 0
        error_msg = ""
        while counter<5:
            try:
                async with session.get(url) as response:
                    text = await response.text()
                    full_row = await self.extract_tags(text,row)
                    return full_row
            except Exception as e:
                error_msg = e
                counter+=1
        print(f"get error:  {error_msg}")
        return row
            
    async def extract_tags(self, text, row):
        try:
            soup = BeautifulSoup(text, 'html.parser')
            tree = etree.HTML(str(soup))
            labels = tree.xpath('//*[@class="displaylabel"]')
            values = tree.xpath('//*[@class="displayvalue"]')
            counter = 1
            for item in labels:
                row.loc[item.text.strip(":")] = values[counter].text
                counter += 2
        except Exception as e:
            print(f"extract error:  {e}")
        return row

    async def main(self, urls):
        tasks = []
        headers = ({'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
            'Accept-Language': 'en-US, en;q=0.5'})
        async with aiohttp.ClientSession(headers=headers) as session:
            for url in urls:
                tasks.append(self.fetch(session, url))
            rows = await asyncio.gather(*tasks)
            for row in rows:
                try:
                    self.df=self.df.append(row,ignore_index=True)
                except Exception as e:
                    print(f"translate error:  {e}")
            print(self.df)            
            
    def get_data(self):
        self.links = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "rb"))
        # self.links = self.links[12:143]
        print(len(self.links))
        segment_size = 100
        for ticker in range (0,len(self.links),segment_size):
            bot_index = ticker+segment_size if ticker+segment_size < len(self.links) else len(self.links)
            asyncio.run(self.main(self.links[ticker:bot_index]))
            pickle.dump(self.df, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "wb"))
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/liq/liq/liq_scrape.csv"
        self.df["Principal's Name"]=self.df["Principal's Name"].str.replace("\n","").str.replace("\r","").str.replace("\t","").str.strip()
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

    def save_pages(self):
        pickle.dump(self.links, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "wb"))
        pickle.dump(self.start_clicks, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/var-liqour-0", "wb"))
        print(self.start_clicks)

    def load_page(self,county):
        url = "https://www.tran.sla.ny.gov/JSP/query/PublicQueryPremisesSearchPage.jsp"
        self.driver.get(url)

        time.sleep(15)

        selector = Select(self.driver.find_element_by_id('county'))
        selector.select_by_visible_text(county)
        
        selectSearch = self.driver.find_element_by_xpath('/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form/div[3]/input')
        selectSearch.click()

        wait = WebDriverWait(self.driver, 10)
        wait.until(lambda driver: self.driver.find_element_by_xpath('/html/body/table/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/div[2]/table/tbody/tr[2]/td[1]/a').is_displayed() == True)
        
        while True:
            try:
                link = self.driver.find_element_by_xpath(f'/html/body/table/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/div[2]/table/tbody/tr[{self.start_clicks[1]}]/td[1]/a').get_attribute("href")
            except:
                break
            self.start_clicks = (self.start_clicks[0] , self.start_clicks[1]+1)
            self.links.append(link)
            if self.start_clicks[1] % 200 == 0:
                self.save_pages()

    def start_scrape(self):
        self.driver = webdriver.Chrome(DirectoryFields.LOCAL_WEBDRIVER_PATH)

        for county in self.county_list:
            if county == self.county_list[self.start_clicks[0]]:
                self.load_page(county)
                self.start_clicks = (self.start_clicks[0]+1,2)
                self.save_pages()

        print("done")

if __name__ == '__main__':
    scraper = LiquorScrape()
    # scraper.start_scrape()
    scraper.get_data()