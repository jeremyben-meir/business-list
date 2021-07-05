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

class BarberAESScrape():
    def __init__(self, industry):
        self.county_list = ['QUEENS','RICHMOND','KINGS','BRONX','NEW YORK']
        self.df = pd.DataFrame(columns=["License Number","Name","Business Name","Address","Zip","Phone","County","License State","License Issue Date","Current Term Effective Date","Expiration Date","Agency","License Status","Industry","URL"])
        self.timeout = 100
        self.segment_size = 100
        self.industry = industry
        self.department = 'dos'
        try:
            self.links = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/links-{self.industry}-0", "rb"))
            self.start_clicks = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/var-{self.industry}-0", "rb"))
        except:
            self.links = []
            self.start_clicks = 1
    
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
            self.df.loc[index,"License Number"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeNumber_value"]')[0].text
            self.df.loc[index,"Name"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblContactName_value"]')[0].text
            self.df.loc[index,"Business Name"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeBusinessName_value"]')[0].text
            addr_list = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeAddress_value"]/text()')
            addr_list = [addr.replace(u'\xa0', u' ').strip() for addr in addr_list]
            self.df.loc[index,"Address"] = addr_list[0]
            self.df.loc[index,"Zip"] = addr_list[1].split(" ")[-1][:5]
            self.df.loc[index,"Phone"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeTelephone1_value"]')[0].text
            self.df.loc[index,"County"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeTitle_value"]')[0].text
            self.df.loc[index,"License State"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeState_value"]')[0].text
            self.df.loc[index,"License Issue Date"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseIssueDate_value"]')[0].text
            self.df.loc[index,"Current Term Effective Date"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblBusinessExpirationDate_value"]')[0].text
            self.df.loc[index,"Expiration Date"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblExpirationDate_value"]')[0].text
            self.df.loc[index,"Agency"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblLicenseeBoard_value"]')[0].text
            self.df.loc[index,"License Status"] = tree.xpath('//*[@id="ctl00_PlaceHolderMain_licenseeGeneralInfo_lblBusinessName2_value"]')[0].text
        except Exception as e:
            print(f"extract error:  {e}")
            self.df.loc[index,"License Number"] = "FAILURE"

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
                # print(top_index)
            await asyncio.gather(*tasks)
            
    def get_data(self, overwrite = True):
        self.links = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/links-{self.industry}-0", "rb"))
        # self.links = self.links[12:143]
        self.df["URL"] = self.links
        self.df["Industry"] = self.industry
        if overwrite == False:
            self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/df-{self.industry}-0", "rb"))
            self.org_df = self.df[~(self.df["License Number"].eq("FAILURE"))]
            self.df = self.df[self.df["License Number"].eq("FAILURE")]
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

        pickle.dump(self.df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/df-{self.industry}-0", "wb"))
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/{self.industry}/{self.department}_{self.industry}_{date.today()}_scrape.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

    def save_pages(self):
        pickle.dump(self.links, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/links-{self.industry}-0", "wb"))
        pickle.dump(self.start_clicks, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/var-{self.industry}-0", "wb"))
        print(self.start_clicks)
        print(len(self.links))

    def wait_until_click(self, elem):
        for _ in range(self.timeout):
            try:
                self.driver.find_element_by_xpath(elem).click()
                return True
            except:
                time.sleep(1)
        return False

    def load_links(self):
        for county in self.county_list:
            self.driver = webdriver.Chrome(DirectoryFields.LOCAL_WEBDRIVER_PATH)
            url = "https://aca.licensecenter.ny.gov/aca/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y"
            wait = WebDriverWait(self.driver, self.timeout)

            self.driver.get(url)

            wait.until(lambda driver:self.driver.find_element_by_id('ctl00_PlaceHolderMain_refLicenseeSearchForm_ddlLicenseType').is_displayed() == True)
            select_license = Select(self.driver.find_element_by_id('ctl00_PlaceHolderMain_refLicenseeSearchForm_ddlLicenseType'))
            select_license.select_by_index(4 if self.industry == 'barber' else 11)
        
            input_city = self.driver.find_element_by_xpath('//*[@id="ctl00_PlaceHolderMain_refLicenseeSearchForm_txtTitle"]')
            input_city.send_keys(county)
            self.driver.find_element_by_id('ctl00_PlaceHolderMain_btnNewSearch').click()

            wait.until(lambda driver:self.driver.find_element_by_xpath(f'//*[@id="ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList"]/tbody/tr[1]').is_displayed() == True)

            def get_top():
                try:
                    return self.driver.find_element_by_xpath(f'//*[@id="ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList"]/tbody/tr[3]/td[1]/div/a').text
                except:
                    time.sleep(1)
                    return self.driver.find_element_by_xpath(f'//*[@id="ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList"]/tbody/tr[3]/td[1]/div/a').text

            topelt = ""
            while True:
                while topelt == get_top():
                    time.sleep(1)
                topelt = self.driver.find_element_by_xpath(f'//*[@id="ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList"]/tbody/tr[3]/td[1]/div/a').text
                num_elts = len(self.driver.find_elements_by_xpath(f'//*[@id="ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList"]/tbody/tr'))
                counter = 3
                while num_elts > 3:
                    text = self.driver.find_element_by_xpath(f'//*[@id="ctl00_PlaceHolderMain_refLicenseeList_gdvRefLicenseeList"]/tbody/tr[{counter}]/td[1]/div/a').text
                    link = f"https://aca.licensecenter.ny.gov/aca/GeneralProperty/LicenseeDetail.aspx?LicenseeNumber={text}&LicenseeType=Appearance%20Enhancement%20Business"
                    num_elts -= 1
                    counter += 1
                    self.links.append(link)
                if not self.wait_until_click('//a[text()="Next >"]'):
                    break

                self.start_clicks += 1
                if self.start_clicks % 10 == 0:
                    self.save_pages()
            self.save_pages()
            self.driver.quit()

        print("done")

if __name__ == '__main__':
    scraper = BarberAESScrape('barber')
    # scraper.load_links()
    scraper.get_data(overwrite=True)