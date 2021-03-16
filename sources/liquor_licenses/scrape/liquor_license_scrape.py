from global_vars import *

from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

def write_to_table(write_list,liq_df,driver):
    liq_df.loc[len(liq_df)] = write_list
    # print(write_list)
    return liq_df
        

def try_click_next(driver):
    entered = False
    retries = 5
    while not entered and retries > 0:
        try:
            driver.find_element_by_name("NextButton").click()
            entered = True
        except:
            retries -= 1
            driver.refresh()

    # print(entered)
    return entered


def read_page(driver,liq_df,county_in):
    table = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")
    skip1 = False
    for length in range(0,len(table)):
        x = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")[length]
        if skip1:
            clicked_in = False
            try:
                x.find_element_by_xpath(".//td[1]/a").click()
                clicked_in = True
            except:
                liq_df = write_to_table(["",x.find_element_by_xpath(".//td[4]").text,x.find_element_by_xpath(".//td[6]").text,"","","",x.find_element_by_xpath(".//td[5]").text,"",x.find_element_by_xpath(".//td[1]").text,"","","",x.find_element_by_xpath(".//td[2]").text,"",x.find_element_by_xpath(".//td[3]").text],liq_df,driver)

            if clicked_in:
                if "does not match any of the licenses" not in driver.page_source:
                    table_table = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/table")
                    serial = ""
                    license_type = ""
                    license_status = ""
                    credit_group = ""
                    filing_date = ""
                    effective_date = ""
                    expiration_date = ""
                    principal_name = ""
                    premises_name = ""
                    trade_name = ""
                    zone = ""
                    county = ""
                    secondary_serial = ""
                    # /html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/table[3]/tbody/tr[6]/td[2]
                    for y in table_table[2].find_elements_by_xpath(".//tbody/tr"):
                        if "Serial Number" in y.find_element_by_xpath(".//td[2]").text:
                            serial = y.find_element_by_xpath(".//td[3]").text
                        elif "License Type" in y.find_element_by_xpath(".//td[2]").text:
                            license_type = y.find_element_by_xpath(".//td[3]").text
                        elif "License Status" in y.find_element_by_xpath(".//td[2]").text:
                            license_status = y.find_element_by_xpath(".//td[3]").text
                        elif "Credit Group" in y.find_element_by_xpath(".//td[2]").text:
                            credit_group = y.find_element_by_xpath(".//td[3]").text
                        elif "Filing Date" in y.find_element_by_xpath(".//td[2]").text:
                            filing_date = y.find_element_by_xpath(".//td[3]").text
                        elif "Effective Date" in y.find_element_by_xpath(".//td[2]").text:
                            effective_date = y.find_element_by_xpath(".//td[3]").text
                        elif "Expiration Date" in y.find_element_by_xpath(".//td[2]").text:
                            expiration_date = y.find_element_by_xpath(".//td[3]").text
                        
                    for y in table_table[-1].find_elements_by_xpath(".//tbody/tr"):
                        if "Principal's" in y.find_element_by_xpath(".//td[2]").text:
                            principal_name = y.find_element_by_xpath(".//td[3]").text
                        elif "Premises Name" in y.find_element_by_xpath(".//td[2]").text:
                            premises_name = y.find_element_by_xpath(".//td[3]").text
                        elif "Trade Name" in y.find_element_by_xpath(".//td[2]").text:
                            trade_name = y.find_element_by_xpath(".//td[3]").text
                        elif "Zone" in y.find_element_by_xpath(".//td[2]").text:
                            zone = y.find_element_by_xpath(".//td[3]").text
                        elif "County" in y.find_element_by_xpath(".//td[2]").text:
                            county = y.find_element_by_xpath(".//td[3]").text
                        
                    
                    try:
                        secondary_serial = driver.find_element_by_xpath("//div[contains(concat(' ', @class, ' '), ' instructions ')]/a").text
                    except:
                        pass
                            
                    driver.back()
                    x = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")[length]
                    liq_df = write_to_table([serial,license_type,license_status,credit_group,filing_date,effective_date,expiration_date,principal_name,premises_name,trade_name,zone,county,x.find_element_by_xpath(".//td[2]").text,secondary_serial,x.find_element_by_xpath(".//td[3]").text],liq_df,driver) 
                else:
                    driver.back()
                    x = driver.find_elements_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/div[2]/table[1]/tbody/tr")[length]
                    liq_df = write_to_table(["",x.find_element_by_xpath(".//td[4]").text,x.find_element_by_xpath(".//td[6]").text,"","","",x.find_element_by_xpath(".//td[5]").text,"",x.find_element_by_xpath(".//td[1]").text,"","",county_in,x.find_element_by_xpath(".//td[2]").text,"",x.find_element_by_xpath(".//td[3]").text],liq_df,driver)
        skip1 = True
    return liq_df


def iterate_through_pages(driver,liq_df,county,start_clicks,county_list):
    clicks = 0
    while True:
        if start_clicks <= clicks:
            liq_df = read_page(driver,liq_df,county)
            print(liq_df)
        if try_click_next(driver):
            clicks += 1
        else:
            pickle.dump(liq_df, open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-2.p", "wb"))
            break
        if start_clicks < clicks:
            pickle.dump(liq_df, open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-2.p", "wb"))
            pickle.dump((county,clicks), open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file2-fin-2.p", "wb"))



def load_page(driver,county):
    url = "https://www.tran.sla.ny.gov/JSP/query/PublicQueryAdvanceSearchPage.jsp"
    driver.get(url)

    time.sleep(15)

    selector = Select(driver.find_element_by_id("county"))
    selector.select_by_visible_text(county)

    selectDate = driver.find_element_by_xpath("/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/div[2]/form/table[5]/tbody/tr/td[2]/table[1]/tbody/tr[4]/td[1]/input")
    selectDate.click()

    inputDate = driver.find_element_by_name('startYear')
    inputDate.send_keys('2012')
    driver.find_element_by_name('startYear').click()

    selectSearch = driver.find_element_by_id('searchButton')
    selectSearch.click()

    wait = WebDriverWait(driver, 10)
    wait.until(lambda driver: driver.find_element_by_xpath('/html/body/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td[2]/table[4]/tbody/tr/td/table/tbody/tr/td[2]/form[2]/table[1]/tbody/tr/td').is_displayed() == True)

def start_scrape():
    finished = True
    while not finished:

        driver = webdriver.Chrome(LOCAL_WEBDRIVER_PATH)

        start_clicks = ("",0)

        try:
            liq_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-2.p", "rb"))
            start_clicks = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file2-fin-2.p", "rb")) #     ("NEW YORK",410) ("KINGS",298) ("RICHMOND",41) ("BRONX",127) ("QUEENS",258)
        except:
            liq_df = pd.DataFrame(columns=["Serial","License Type","License Status","Credit Group","Filing Date","Effective Date","Expiration Date","Principal's Name","Premises Name","Trade Name","Zone","County","Address Tab","Serial 2","License Class"])
            start_clicks = ("RICHMOND",0)

        county_list = ["RICHMOND","NEW YORK","QUEENS","KINGS","BRONX"]
        has_reached = False
        
        try:
            for county in county_list:
                if county == start_clicks[0]:
                    has_reached = True
                if has_reached:
                    load_page(driver,county)
                    iterate_through_pages(driver,liq_df,county,start_clicks[1],county_list)
                    start_clicks = (county,0)
            finished = True
        except:
            driver.close()

    end_file = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-2.p", "rb"))
    end_file.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/liquors_scraped.csv",index=False,quoting=csv.QUOTE_ALL)

if __name__ == '__main__':
    start_scrape()