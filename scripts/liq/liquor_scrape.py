from scripts.common import DirectoryFields
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from lxml import etree
from scripts.scrape_file import ScrapeFile, pd, sys, time

class LiquorScrape(ScrapeFile):
    def __init__(self):
        df = pd.DataFrame(columns=["Premises Name","Trade Name","Zone","Address","Zip","County","Serial Number","License Type","License Status","Credit Group","Filing Date","Effective Date","Expiration Date","Principal's Name","URL","Status"])#,"Serial 2","License Class"])
        super().__init__(df=df,timeout=100,segment_size=500, department = 'liq', filename = 'liquor', start_clicks=(0,2))
            
    async def extract_tags(self, text, index):
        # val = 0
        # start = time.time()
        try:
            soup = BeautifulSoup(text, 'html.parser')
            tree = etree.HTML(str(soup))
            # val += 1 
            labels = tree.xpath('//*[@class="displaylabel"]')
            if len(labels) > 0:
                values = tree.xpath('//*[@class="displayvalue"]')
                # stop = time.time()
                # val += 1 
                if labels[0].text.strip(":") != "Serial Number":
                    # val += 1
                    # print(stop-start)
                    self.df.loc[index,"Serial Number"] = tree.xpath('//*[@class="instructions"]/a')[0].text
                counter = 1
                # val += 1 
                for item in labels:
                    if item.text.strip(":") == "Address":
                        self.df.loc[index,"Address"] = values[counter].text
                        self.df.loc[index,"Zip"] = values[counter+4].text.split(" ")[-1][:5]
                        counter += 6
                    else:
                        self.df.loc[index,item.text.strip(":")] = values[counter].text
                        counter += 2
            else:
                self.df.loc[index,"Status"] = "FAILURE"
        except Exception as e:

            print(f"extract error: {e}")
            # if type(e).__name__ == "IndexError":
            #     self.df = self.df.drop(index)
            # else:
            self.df.loc[index,"Status"] = "FAILURE"

    def load_links(self):
        county_list = ['QUEENS','RICHMOND','KINGS','BRONX','NEW YORK']
        self.driver = webdriver.Chrome(DirectoryFields.LOCAL_WEBDRIVER_PATH,options=self.options)

        print(self.start_clicks)
        for county in county_list:
            if county == county_list[self.start_clicks[0]]:
                url = "https://www.tran.sla.ny.gov/JSP/query/PublicQueryPremisesSearchPage.jsp"
                self.driver.get(url)

                time.sleep(15)
                # print(1)
                selector = Select(self.driver.find_element_by_id('county'))
                selector.select_by_visible_text(county)
                
                # print(2)
                selectSearch = self.driver.find_element_by_xpath('/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form/div[3]/input')
                selectSearch.click()

                # print(3)
                wait = WebDriverWait(self.driver, 10)
                wait.until(lambda driver: self.driver.find_element_by_xpath('/html/body/table/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/div[2]/table/tbody/tr[2]/td[1]/a').is_displayed() == True)
                
                # print(4)
                while True:
                    try:
                        link = self.driver.find_element_by_xpath(f'/html/body/table/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/div[2]/table/tbody/tr[{self.start_clicks[1]}]/td[1]/a').get_attribute("href")
                    except:
                        # print(5)
                        break
                    self.start_clicks = (self.start_clicks[0] , self.start_clicks[1]+1)
                    self.links.append(link)
                    # print(link)
                    if self.start_clicks[1] % 200 == 0:
                        self.save_pages()

                self.start_clicks = (self.start_clicks[0]+1,2)
                self.save_pages()
        print("done")

if __name__ == '__main__':
    scraper = LiquorScrape()
    # scraper.load_links()
    scraper.get_data(overwrite = False)