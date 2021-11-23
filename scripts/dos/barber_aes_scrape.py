from scripts.common import DirectoryFields
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from lxml import etree
from scripts.scrape_file import ScrapeFile, pd, sys, time

class BarberAESScrape(ScrapeFile):
    def __init__(self, filename):
        df = pd.DataFrame(columns=["License Number","Name","Business Name","Address","Zip","Phone","County","License State","License Issue Date","Current Term Effective Date","Expiration Date","Agency","License Status","Industry","URL","Status"])
        super().__init__(df=df,timeout=100,segment_size=100, department = 'dos', filename = filename)

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
            self.df.loc[index,"Industry"] = self.filename
        except Exception as e:
            print(f"extract error:  {e}")
            self.df.loc[index,"Status"] = "FAILURE"

    def load_links(self):
        county_list = ['QUEENS','RICHMOND','KINGS','BRONX','NEW YORK']
        for county in county_list:
            self.driver = webdriver.Chrome(DirectoryFields.LOCAL_WEBDRIVER_PATH, options=self.options)
            url = "https://aca.licensecenter.ny.gov/aca/GeneralProperty/PropertyLookUp.aspx?isLicensee=Y"
            wait = WebDriverWait(self.driver, self.timeout)

            self.driver.get(url)

            wait.until(lambda driver:self.driver.find_element_by_id('ctl00_PlaceHolderMain_refLicenseeSearchForm_ddlLicenseType').is_displayed() == True)
            select_license = Select(self.driver.find_element_by_id('ctl00_PlaceHolderMain_refLicenseeSearchForm_ddlLicenseType'))
            select_license.select_by_index(4 if self.filename == 'aes' else 11)
        
            try:
                input_city = self.driver.find_element_by_xpath('//*[@id="ctl00_PlaceHolderMain_refLicenseeSearchForm_txtTitle"]')
                input_city.send_keys(county)
                self.driver.find_element_by_id('ctl00_PlaceHolderMain_btnNewSearch').click()
            except:
                sys.exit("County option not present")

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
                    link = f"https://aca.licensecenter.ny.gov/aca/GeneralProperty/LicenseeDetail.aspx?LicenseeNumber={text}&LicenseeType={'Appearance Enhancement Business' if self.filename == 'aes' else 'Barber Shop'}"
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
    scraper = BarberAESScrape('aes')
    scraper.load_links()
    scraper.get_data(overwrite=True)

    scraper = BarberAESScrape('barber')
    scraper.load_links()
    scraper.get_data(overwrite=True)