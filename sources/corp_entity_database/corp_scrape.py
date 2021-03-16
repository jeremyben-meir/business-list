from global_vars import *
from captcha_breaker import solve_captcha
import urllib
import urllib.request
import multiprocessing
from multiprocessing import Process
from multiprocessing import Manager
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
import difflib

class Spider():

    def __init__(self, thread_count,charset):
        self.thread_count = thread_count
        self.charset = charset

    def search_page(self,search_value):
        print(search_value)
        while True:
            selector = Select(self.driver.find_element_by_id('p_name_type'))
            selector.select_by_visible_text("All")

            img = self.driver.find_element_by_xpath("/html/body/center/form/div/fieldset/img")
            src = img.get_attribute('src')
            urllib.request.urlretrieve(src, LOCAL_LOCUS_PATH + "captcha_breaker/" + str(self.thread_count) + ".png")

            result_string = solve_captcha.solve(LOCAL_LOCUS_PATH + "captcha_breaker/" + str(self.thread_count) + ".png")

            result_string = "11111" if result_string == "" else result_string

            captcha_bar = self.driver.find_element_by_id('p_captcha')
            captcha_bar.clear()
            captcha_bar.send_keys(result_string)

            search_bar = self.driver.find_element_by_id('p_entity_name')
            search_bar.clear()
            search_bar.send_keys(search_value)

            self.driver.find_element_by_xpath("/html/body/center/form/input").click()

            if "CAPTCHA value does not match the image" in self.driver.page_source:
                self.driver.find_element_by_xpath("/html/body/center/div[1]/div/a").click()
            else:
                break
        
        if "No business entities were found" in self.driver.page_source:
            self.driver.back()
            print(search_value + ": 0")
            return 0
        elif "More than 500" in self.driver.find_element_by_class_name("messages").text:
            self.driver.back()
            print(search_value + ": >500")
            return 1000
        else:
            if "Only one" in self.driver.find_element_by_class_name("messages").text:
                to_return = 1
            else:
                to_return = int(re.sub("\D", "", self.driver.find_element_by_class_name("messages").text))
            print(search_value + ": " + str(to_return))
            return to_return

    def can_click_next(self):
        entered = False
        retries = 5
        while not entered and retries > 0:
            try:
                self.driver.find_element_by_xpath("//*[contains(text(), 'Next Page')]").click()
                entered = True
            except:
                retries -= 1
                return False
                #self.driver.refresh()
                
        return entered

    def title_index(self,title):
        if "DOS Process" in title:
            return 0
        if "Chief Executive Officer" in title:
            return 1
        if "Principal Executive Office" in title:
            return 2
        return 3
        
    def scrape_corp_page(self):
        to_append = []

        table1 = self.driver.find_elements_by_xpath("/html/body/center/table[1]/tbody/tr")
        for x in range(1,len(table1)+1):
            to_append.append(self.driver.find_element_by_xpath("/html/body/center/table[1]/tbody/tr["+str(x)+"]/td").text)
        
        table2 = self.driver.find_elements_by_xpath("//*[@id='tblAddr']/tbody/tr")
        adder = 0
        for x in range(1,len(table2)+1,2):
            cur_index = (x - 1)/2
            cur_title = self.driver.find_element_by_xpath("//*[@id='tblAddr']/tbody/tr["+str(x)+"]/th").text
            # print(title_index(cur_title))
            # print(cur_index+adder)
            for _ in range (0, int(self.title_index(cur_title) - (cur_index+adder))):
                to_append.append("")
                adder+=1
            to_append.append(self.driver.find_element_by_xpath("//*[@id='tblAddr']/tbody/tr["+str(x+1)+"]/td").text)
        
        to_append.append(self.driver.find_element_by_xpath("//*[@id='tblStock']/tbody/tr[2]/td[1]").text)
        to_append.append(self.driver.find_element_by_xpath("//*[@id='tblStock']/tbody/tr[2]/td[2]").text)
        to_append.append(self.driver.find_element_by_xpath("//*[@id='tblStock']/tbody/tr[2]/td[3]").text)

        table3 = self.driver.find_elements_by_xpath("//*[@id='tblNameHist']/tbody/tr")
        row1 = []
        row2 = []
        row3 = []
        for x in range(2,len(table3)+1):
            row1.append(self.driver.find_element_by_xpath("//*[@id='tblNameHist']/tbody/tr["+str(x)+"]/td[1]").text)
            row2.append(self.driver.find_element_by_xpath("//*[@id='tblNameHist']/tbody/tr["+str(x)+"]/td[2]").text)
            row3.append(self.driver.find_element_by_xpath("//*[@id='tblNameHist']/tbody/tr["+str(x)+"]/td[3]").text)

        to_append.append("\$L$/".join(row1))
        to_append.append("\$L$/".join(row2))
        to_append.append("\$L$/".join(row3))

        # print(to_append)
        return to_append


    def crawl_on_search(self):
        temp_df = self.my_df
        while True:
            
            table = self.driver.find_elements_by_xpath("/html/body/center/table/tbody/tr")
            for x in range(2, len(table)+1):
                self.driver.find_element_by_xpath("/html/body/center/table/tbody/tr["+str(x)+"]/td/a").click()
                temp_df.loc[len(temp_df)] = self.scrape_corp_page()
                self.driver.back()

            if not self.can_click_next():
                print(temp_df)
                try:
                    self.driver.find_element_by_xpath("//div[contains(concat(' ', @class, ' '), ' linkbutton ')][2]/a[1]").click()
                except:
                    self.driver.find_element_by_xpath("//div[contains(concat(' ', @class, ' '), ' linkbutton ')]/a[1]").click()
                break
        return temp_df


    def begin_char_search(self,char):
        total = 'tbcd6afgh7eijkl8mno9pqsruvwxyz~!@#$%&*()_+`?/.,|[]\-;:"123450 '

        if char not in self.charlist:
            num_results = self.search_page(char)
            if num_results == 1000:
                for added_char in total:
                    if added_char != " ":
                        self.begin_char_search(char+added_char)
                    else:
                        for added_char_2 in total[:-1]:
                            self.begin_char_search(char+" "+added_char_2)

            elif num_results != 0:
                self.my_df = self.crawl_on_search()
            
            self.charlist.append(char)
                

    def process_segment(self):
        url = "https://appext20.dos.ny.gov/corp_public/corpsearch.entity_search_entry"
        while True:
            self.driver = webdriver.Chrome(LOCAL_WEBDRIVER_PATH)
            self.driver.get(url)

            try:
                self.my_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/df-"+str(self.thread_count)+".p","rb"))
                self.charlist = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/charlist-"+str(self.thread_count)+".p","rb"))
            except:
                self.my_df = pd.DataFrame(columns=['Current Entity Name', 'DOS ID', 'Initial DOS Filing Date', 'County', 'Jurisdiction', 'Entity Type', 'Current Entity Status', 'DOS Process', 'Chief Executive Officer', 'Principal Executive Office', 'Registered Agent', 'Number of Shares', 'Type of Stock', 'Value per Share', 'Filing Date', 'Name Type', 'Entity Name']) 
                self.charlist = []
            
            try:
                for char in self.charset:
                    self.begin_char_search(char)
            except:
                print("error")
                pickle.dump(self.my_df,open(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/df-"+str(self.thread_count)+".p","wb"))
                pickle.dump(self.charlist,open(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/charlist-"+str(self.thread_count)+".p","wb"))
                self.driver.close()

def begin_scrape():

    total_file_path = LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/corporation_list.csv"

    charset = ['t','bcd6','afgh7','eijkl8','mno9','pqs','ruvwxyz','~!@#$%&*()_+`?/.,|[]\-;:"123450']
    charset = ['t','bcd6','afgh7','eijkl8']
    charset = ['t','b','a','ei']
    # charset = ['t']

    thread_list = []
    thread_count = 0
    for x in charset:
        thread_list.append(Spider(thread_count=thread_count,charset=x))
        thread_count+=1

    process_list = []
    for x in thread_list:
        process_list.append(Process(target=x.process_segment))
        process_list[-1].start()

    for x in process_list:
        x.join()

if __name__ ==  "__main__":
    begin_scrape()