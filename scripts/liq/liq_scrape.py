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
from datetime import date

class LiquorScrape():
    def __init__(self):
        self.county_list = ['QUEENS','RICHMOND','KINGS','BRONX','NEW YORK']
        self.df = pd.DataFrame(columns=["Premises Name","Trade Name","Zone","Address","Zip","County","Serial Number","License Type","License Status","Credit Group","Filing Date","Effective Date","Expiration Date","Principal's Name","URL"])#,"Serial 2","License Class"])
        self.segment_size = 100
        self.department = 'liq'
        try:
            # self.df = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "rb"))
            self.links = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "rb"))
            self.start_clicks = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/var-liqour-0", "rb"))
        except:
            self.links = []
            self.start_clicks = (0,2)
    
    async def fetch(self, session, index, row):
        success = False
        err_msg = ""
        for _ in range(5):
            try:
                async with session.get(row["URL"]) as response:
                    text = await response.text()
                    await self.extract_tags(text,index)
                    success = True
                    break
            except Exception as e:
                err_msg = e
        if not success:
            print(f"get error: {err_msg}")
            self.df.loc[index,"License Number"] = "FAILURE"
            
    async def extract_tags(self, text, index):
        try:
            soup = BeautifulSoup(text, 'html.parser')
            tree = etree.HTML(str(soup))
            labels = tree.xpath('//*[@class="displaylabel"]')
            values = tree.xpath('//*[@class="displayvalue"]')
            if labels[0].text.strip(":") != "Serial Number":
                self.df.loc[index,"Serial Number"] = tree.xpath('//*[@class="instructions"]/a')[0].text
            counter = 1
            for item in labels:
                if item.text.strip(":") == "Address":
                    self.df.loc[index,"Address"] = values[counter].text
                    self.df.loc[index,"Zip"] = values[counter+4].text.split(" ")[-1][:5]
                    counter += 6
                else:
                    self.df.loc[index,item.text.strip(":")] = values[counter].text
                    counter += 2
        except Exception as e:
            print(f"extract error:  {e}")
            self.df.loc[index,"Premises Name"] = "FAILURE"

    async def main(self, top_index, bot_index):
        tasks = list()
        start_index = top_index
        headers = ({'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
            'Accept-Language': 'en-US, en;q=0.5'})
        async with aiohttp.ClientSession(headers=headers) as session:
            while top_index <= bot_index and top_index - start_index < self.segment_size:
                task = asyncio.ensure_future(self.fetch(session, top_index, self.df.loc[top_index]))
                tasks.append(task)
                top_index+=1
            await asyncio.gather(*tasks)          
            
    def get_data(self, overwrite = True):
        self.links = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "rb"))
        # self.links = self.links[12:143]
        self.df["URL"] = self.links
        if overwrite == False:
            self.df = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "rb")) 
            self.org_df = self.df[~(self.df["Premises Name"].eq("FAILURE"))]
            self.df = self.df[self.df["Premises Name"].eq("FAILURE")]
        print(len(self.df))
        self.df=self.df.reset_index(drop=True)
        
        top_index = self.df.iloc[0].name
        bot_index = self.df.iloc[-1].name
        while top_index <= bot_index:
            asyncio.run(self.main(top_index,bot_index))
            top_index+=self.segment_size
            print(top_index)

        if overwrite == False:
            self.df = pd.concat([self.org_df,self.df], ignore_index=True)

        pickle.dump(self.df, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "wb"))
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/liq/liq/{self.department}_liquor_{date.today()}_scrape.csv"
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
    scraper.get_data(overwrite = True)