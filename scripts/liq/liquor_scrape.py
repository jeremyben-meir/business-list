from scripts.common import DirectoryFields
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from lxml import etree
from scripts.scrape_file import ScrapeFile, pd, sys, time

class LiquorScrape(ScrapeFile):
    def __init__(self):
        df = pd.DataFrame(columns=["Premises Name","Trade Name","Zone","Address","Zip","County","Serial Number","License Type","License Status","Credit Group","Filing Date","Effective Date","Expiration Date","Principal's Name","URL"])#,"Serial 2","License Class"])
        super().__init__(df=df,timeout=100,segment_size=100, department = 'liq', filename = 'liquor', start_clicks=(0,2))
            
    async def extract_tags(self, text, index):
        try:
            soup = BeautifulSoup(text, 'html.parser')
            tree = etree.HTML(str(soup))
            labels = tree.xpath('//*[@class="displaylabel"]')
            values = tree.xpath('//*[@class="displayvalue"]')
            print([label.text for label in labels])
            if len(labels) == 0:
                print(self.df.loc[index,"URL"])
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
            print(type(e).__name__)
            if type(e).__name__ == "IndexError":
                del self.df.loc[index]
            else:
                self.df.loc[index,"Premises Name"] = "FAILURE"

    def load_links(self):
        county_list = ['QUEENS','RICHMOND','KINGS','BRONX','NEW YORK']
        self.driver = webdriver.Chrome(DirectoryFields.LOCAL_WEBDRIVER_PATH)

        print(self.start_clicks)
        for county in county_list:
            if county == county_list[self.start_clicks[0]]:
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

                self.start_clicks = (self.start_clicks[0]+1,2)
                self.save_pages()
        print("done")

if __name__ == '__main__':
    scraper = LiquorScrape()
    # scraper.load_links()
    scraper.get_data(overwrite = False)