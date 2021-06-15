from global_vars import LOCAL_LOCUS_PATH
from classes.Counter import Counter
import json
import requests
import pandas as pd
import asyncio
import aiohttp
from multiprocessing import Process
import time
import concurrent.futures

class BBLAdder:

    def __init__(self, app_id, app_key, df, adder_id, overwrite, segment_size):
        self.app_id = app_id
        self.app_key = app_key
        self.df = df
        self.adder_id = adder_id
        self.overwrite = overwrite
        self.segment_size = segment_size

        self.index_counter = 0
        self.beg_time = time.time()
        if self.adder_id == 0:
            self.counter = Counter(len(df), precision=2)
        

    async def counter_max(self):
        self.index_counter += 1
        if self.index_counter >= 2500:
            self.index_counter = 0
            time_count = time.time() - self.beg_time
            if time_count < 60:
                time.sleep(61-time_count)
            self.beg_time = time.time()
        return
            

    async def get_raw(self, session, inject):
        url = "https://api.cityofnewyork.us/geoclient/v1/search.json?input=" + inject + "&app_id=" + self.app_id + "&app_key=" + self.app_key
        async with session.get(url) as response:
            results = await response.json()
            await self.counter_max()
            return results


    async def get_result(self, session, inject):
        results = await self.get_raw(session, inject)
        return results['results'][0]['response']['bbl']

    async def decide_result(self, session, index, row): 
        if self.overwrite or len(self.df.loc[index,"BBL"])==0:
            try:
                self.df.loc[index,"BBL"] = await self.get_result(session, row['Building Number'] + " " + row['Street'] + " " + row['City'])
            except:
                try:
                    self.df.loc[index,"BBL"] = await self.get_result(session, row['Building Number'] + " " + row['Street'] + " " + row['Zip'])
                except:
                    self.df.loc[index,"BBL"]=""
        if self.adder_id==0:
            self.counter.tick()

    async def add_bbl_helper(self,top_index,bot_index):
        async with aiohttp.ClientSession() as session:
            tasks = []
            start_index = top_index
            while top_index <= bot_index and top_index - start_index < self.segment_size:
                task = asyncio.ensure_future(self.decide_result(session,top_index,self.df.loc[top_index]))
                tasks.append(task)
                top_index+=1
            await asyncio.gather(*tasks)

    def add_bbl_starter(self):  # input row must have headers 'Building Number,' 'Street,' 'City,' 'Zip,' 'BBL'
        top_index = self.df.iloc[0].name
        bot_index = self.df.iloc[-1].name
        while top_index <= bot_index:
            asyncio.run(self.add_bbl_helper(top_index,bot_index))
            top_index+=self.segment_size
        return self.df


    ###################################################################################################

    # def increment_global_key(self):
    #     self.key_row += 1
    #     if self.key_row >= len(self.keylist):
    #         self.key_row = 0
        
    # def add_bbl(self, df, overwrite=True):  # input row must have headers 'Building Number,' 'Street,' 'City,' 'Zip,' 'BBL'
            
    #     def get_raw(inject):
    #         curkey = self.keylist[self.key_row][0]
    #         curid = self.keylist[self.key_row][1]
    #         url = "https://api.cityofnewyork.us/geoclient/v1/search.json?input=" + inject + "&app_id=" + curid + "&app_key=" + curkey
    #         response = requests.get(url)
    #         decoded = response.content.decode("utf-8")
    #         return decoded

    #     def get_result(inject):
    #         results = get_raw(inject)
    #         cap = 0
    #         while results == "Authentication failed" and cap < len(self.keylist):
    #             print("KEY " + str(self.key_row) + " EXPIRED")
    #             self.increment_global_key()
    #             results = get_raw(inject)
    #             cap += 1
    #         if results == "Authentication failed":
    #             print("ALL KEYS EXPIRED")
    #             return("NO-KEY")
    #         json_loaded = json.loads(results)
    #         return json_loaded['results'][0]['response']['bbl']

    #     def decide_result(row): 
    #         if overwrite or len(row["BBL"])==0:
    #             try:
    #                 row["BBL"] = get_result(row['Building Number'] + " " + row['Street'] + " " + row['City'])
    #             except:
    #                 try:
    #                     row["BBL"] = get_result(row['Building Number'] + " " + row['Street'] + " " + row['Zip'])
    #                 except:
    #                     row["BBL"]=""

    #         self.counter.tick()
    #         return row

    #     self.init_ticker(len(df), precision=2)
    #     df = df.apply(lambda row: decide_result(row), axis=1)
    
    #     return df