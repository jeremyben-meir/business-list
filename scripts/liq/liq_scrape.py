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
        self.df = pd.DataFrame(columns=["Premises Name","Trade Name","Zone","Address","County","Serial Number","License Type","License Status","Credit Group","Filing Date","Effective Date","Expiration Date","Principal's Name"])#,"Serial 2","License Class"])

    # def write_to_table(self, write_list,liq_df,driver):
    #     liq_df.loc[len(liq_df)] = write_list
    #     return liq_df
            

    # def try_click_next(self, driver):
    #     entered = False
    #     retries = 5
    #     while not entered and retries > 0:
    #         try:
    #             driver.find_element_by_name("NextButton").click()
    #             entered = True
    #         except:
    #             retries -= 1
    #             driver.refresh()

    #     return entered

    # def read_page(self, driver,liq_df,county_in):
    #     table = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")
    #     skip1 = False
    #     for length in range(0,len(table)):
    #         x = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")[length]
    #         if skip1:
    #             clicked_in = False
    #             try:
    #                 x.find_element_by_xpath(".//td[1]/a").click()
    #                 clicked_in = True
    #             except:
    #                 liq_df = write_to_table(["",x.find_element_by_xpath(".//td[4]").text,x.find_element_by_xpath(".//td[6]").text,"","","",x.find_element_by_xpath(".//td[5]").text,"",x.find_element_by_xpath(".//td[1]").text,"","","",x.find_element_by_xpath(".//td[2]").text,"",x.find_element_by_xpath(".//td[3]").text],liq_df,driver)

    #             if clicked_in:
    #                 if "does not match any of the licenses" not in driver.page_source:
    #                     table_table = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/table")
    #                     serial = ""
    #                     license_type = ""
    #                     license_status = ""
    #                     credit_group = ""
    #                     filing_date = ""
    #                     effective_date = ""
    #                     expiration_date = ""
    #                     principal_name = ""
    #                     premises_name = ""
    #                     trade_name = ""
    #                     zone = ""
    #                     county = ""
    #                     secondary_serial = ""
    #                     for y in table_table[2].find_elements_by_xpath(".//tbody/tr"):
    #                         if "Serial Number" in y.find_element_by_xpath(".//td[2]").text:
    #                             serial = y.find_element_by_xpath(".//td[3]").text
    #                         elif "License Type" in y.find_element_by_xpath(".//td[2]").text:
    #                             license_type = y.find_element_by_xpath(".//td[3]").text
    #                         elif "License Status" in y.find_element_by_xpath(".//td[2]").text:
    #                             license_status = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Credit Group" in y.find_element_by_xpath(".//td[2]").text:
    #                             credit_group = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Filing Date" in y.find_element_by_xpath(".//td[2]").text:
    #                             filing_date = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Effective Date" in y.find_element_by_xpath(".//td[2]").text:
    #                             effective_date = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Expiration Date" in y.find_element_by_xpath(".//td[2]").text:
    #                             expiration_date = y.find_element_by_xpath(".//td[3]").text
                            
    #                     for y in table_table[-1].find_elements_by_xpath(".//tbody/tr"):
    #                         if "Principal's" in y.find_element_by_xpath(".//td[2]").text:
    #                             principal_name = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Premises Name" in y.find_element_by_xpath(".//td[2]").text:
    #                             premises_name = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Trade Name" in y.find_element_by_xpath(".//td[2]").text:
    #                             trade_name = y.find_element_by_xpath(".//td[3]").text
    #                         elif "Zone" in y.find_element_by_xpath(".//td[2]").text:
    #                             zone = y.find_element_by_xpath(".//td[3]").text
    #                         elif "County" in y.find_element_by_xpath(".//td[2]").text:
    #                             county = y.find_element_by_xpath(".//td[3]").text
                            
                        
    #                     try:
    #                         secondary_serial = driver.find_element_by_xpath("//div[contains(concat(' ', @class, ' '), ' instructions ')]/a").text
    #                     except:
    #                         pass
                                
    #                     driver.back()
    #                     x = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")[length]
    #                     liq_df = write_to_table([serial,license_type,license_status,credit_group,filing_date,effective_date,expiration_date,principal_name,premises_name,trade_name,zone,county,x.find_element_by_xpath(".//td[2]").text,secondary_serial,x.find_element_by_xpath(".//td[3]").text],liq_df,driver) 
    #                 else:
    #                     driver.back()
    #                     x = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")[length]
    #                     liq_df = write_to_table(["",x.find_element_by_xpath(".//td[4]").text,x.find_element_by_xpath(".//td[6]").text,"","","",x.find_element_by_xpath(".//td[5]").text,"",x.find_element_by_xpath(".//td[1]").text,"","",county_in,x.find_element_by_xpath(".//td[2]").text,"",x.find_element_by_xpath(".//td[3]").text],liq_df,driver)
    #         skip1 = True
    #     return liq_df

    # def iterate_through_pages(self,driver,liq_df,county,start_clicks,county_list):
    #     clicks = 0
    #     while True:
    #         if start_clicks <= clicks:
    #             liq_df = read_page(driver,liq_df,county)
    #             print(liq_df)
    #         if try_click_next(driver):
    #             clicks += 1
    #         else:
    #             pickle.dump(liq_df, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/file-fin-2.p", "wb"))
    #             break
    #         if start_clicks < clicks:
    #             pickle.dump(liq_df, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/file-fin-2.p", "wb"))
    #             pickle.dump((county,clicks), open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/file2-fin-2.p", "wb"))

    
    async def fetch(self, session, url):    
        try:
            async with session.get(url) as response:
                text = await response.text()
                tags = await self.extract_tags(text)
                return tags
        except Exception as e:
            print(f"get error:  {e}")
            return [url]
            
    async def extract_tags(self, text): ##EDIT CONTENT
        try:
            soup = BeautifulSoup(text, 'html.parser')
            tree = etree.HTML(str(soup))
            labels = tree.xpath('//*[@class="displaylabel"]')
            values = tree.xpath('//*[@class="displayvalue"]')
            counter = 1
            # dflen = len(self.df)
            # if dflen % 100 == 0:
            #     print(dflen)
            val_list = []
            if len(labels) != 13:
                print(len(labels))
            for item in labels:
                # print(f"{item.text} {values[counter].text}")
                # self.df.loc[dflen,item.text.strip(":")] = values[counter].text
                val_list.append(values[counter].text)
                counter += 2
            return val_list
        except Exception as e:
            print(f"extract error:  {e}")
            return []

    async def main(self, urls):
        tasks = []
        headers = ({'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
            'Accept-Language': 'en-US, en;q=0.5'})
        async with aiohttp.ClientSession(headers=headers) as session:
            for url in urls:
                tasks.append(self.fetch(session, url))
            htmls = await asyncio.gather(*tasks)
            # for html in htmls:
            #     if html is not None:
            #         print(len(html))
            # print(len(htmls))
            self.df=pd.DataFrame(htmls, columns=self.df.columns).append(self.df, ignore_index=True)
            print(self.df)
            
    def get_data(self):
        self.links = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "rb"))
        print(len(self.links))
        segment_size = 1000
        for ticker in range (0,len(self.links),segment_size):
            bot_index = ticker+segment_size if ticker+segment_size < len(self.links) else len(self.links)
            asyncio.run(self.main(self.links[ticker:bot_index]))
            print(ticker)
            pickle.dump(self.df, open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "wb"))
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/liq/temp/liq_scrape.csv"
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

        try:
            # self.df = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/df-liqour-0", "rb"))
            self.links = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/links-liqour-0", "rb"))
            self.start_clicks = pickle.load(open(DirectoryFields.LOCAL_LOCUS_PATH + "data/liq/temp/var-liqour-0", "rb"))
        except:
            self.links = []
            self.start_clicks = (0,2)

        for county in self.county_list:
            if county == self.county_list[self.start_clicks[0]]:
                self.load_page(county)
                self.start_clicks = (self.start_clicks[0]+1,2)
                self.save_pages()

        print("done")

if __name__ == '__main__':
    scraper = LiquorScrape()
    scraper.get_data()