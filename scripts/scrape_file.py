from scripts.common import DirectoryFields
import pandas as pd
import aiohttp
import asyncio
import pickle
import time
from datetime import date
import csv
import sys
import urllib.request
import boto3
from io import BytesIO
import gzip
from selenium.webdriver.chrome.options import Options

class ScrapeFile:

    def __init__(self, df, timeout, segment_size, department, filename, start_clicks=1):
        self.s3 = boto3.resource('s3')
        self.df = df
        self.timeout = timeout
        self.segment_size = segment_size
        self.department = department
        self.filename = filename
        self.options = Options()
        self.options.headless = True

        try:
            self.links = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(f"data/{self.department}/temp/links-{self.filename}").get()['Body'].read())
            self.start_clicks = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(f"data/{self.department}/temp/var-{self.filename}").get()['Body'].read())

            # self.links = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/links-{self.filename}", "rb"))
            # self.start_clicks = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/var-{self.filename}", "rb"))
        except:
            self.links = []
            self.start_clicks = start_clicks


    def wait_until_click(self, elem):
        for _ in range(self.timeout):
            try:
                self.driver.find_element_by_xpath(elem).click()
                return True
            except:
                time.sleep(1)
        return False

    async def fetch(self, session, index, row):
        try:
            async with session.get(row["URL"]) as response:
                text = await response.text()
                await self.extract_tags(text,index)
        except:
            self.df.loc[index,"Complete"] = "FAILURE"

        # success = False
        # err_msg = ""
        # for _ in range(5):
        #     try:
        #         async with session.get(row["URL"]) as response:
        #             text = await response.text()
        #             await self.extract_tags(text,index)
        #             success = True
        #             break
        #     except Exception as e:
        #         err_msg = e
                
        # if not success:
        #     print(f"get error: {err_msg}")
        #     self.df.loc[index,self.first_col] = "FAILURE"

    async def extract_tags(self, text, index):
        sys.exit("Accessing parent class. Should instead call child class.")

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
            len(self.df["Complete"][start_index:top_index])
            self.df["Complete"][start_index:top_index] = "SUCCESS"
            self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"data/{self.department}/temp/df-{self.filename}", Body=pickle.dumps(self.df))
        
    def get_data(self, overwrite = True):
        self.links = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(f"data/{self.department}/temp/links-{self.filename}").get()['Body'].read())
        # self.links = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/links-{self.filename}", "rb"))
        # self.links = self.links[12:143]
        self.df["URL"] = self.links
        self.df["Complete"] = "FAILURE"
        if overwrite == False:
            self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(f"data/{self.department}/temp/df-{self.filename}").get()['Body'].read())
            # self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/df-{self.filename}", "rb"))
            self.org_df = self.df[~(self.df["Complete"].eq("FAILURE"))]
            self.df = self.df[self.df["Complete"].eq("FAILURE")]
            # print(self.org_df[""])
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

        # pickle.dump(self.df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/df-{self.filename}", "wb"))
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"data/{self.department}/temp/df-{self.filename}", Body=pickle.dumps(self.df))
        cleaned_file_path = f"{DirectoryFields.S3_PATH}data/{self.department}/{self.filename}/{self.department}_{self.filename}_{date.today()}_scrape.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

    def save_pages(self):
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"data/{self.department}/temp/links-{self.filename}", Body=pickle.dumps(self.links))
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"data/{self.department}/temp/var-{self.filename}", Body=pickle.dumps(self.start_clicks))
        # pickle.dump(self.links, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/links-{self.filename}", "wb"))
        # pickle.dump(self.start_clicks, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/var-{self.filename}", "wb"))
        print(f"clicks {self.start_clicks}")
        print(f"links {len(self.links)}")

if __name__ == "__main__":
    scrape_file = ScrapeFile(None, None, None, None, None)
    scrape_file.download_chromium()