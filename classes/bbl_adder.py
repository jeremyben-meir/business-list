from classes.common import DirectoryFields
from classes.counter import Counter
import json
import requests
import pandas as pd
import asyncio
import aiohttp
from multiprocessing import Process
import time
import concurrent.futures

class BBLAdder:

    def __init__(self, df, overwrite, keylist):
        self.keylist = keylist
        self.key_row = 0
        self.app_key = self.keylist[self.key_row][0]
        self.app_id = self.keylist[self.key_row][1]
        self.df = df
        self.overwrite = overwrite
        self.segment_size = 2500
        self.index_counter = 0
        self.counter = Counter(len(df), precision=2)

    def increment_global_key(self):
        self.key_row += 1
        if self.key_row >= len(self.keylist):
            self.key_row = 0
        self.app_key = self.keylist[self.key_row][0]
        self.app_id = self.keylist[self.key_row][1]
            
    async def get_result(self, session, inject):
        url = f"https://api.cityofnewyork.us/geoclient/v1/search.json?input={inject}&app_id={self.app_id}&app_key={self.app_key}"
        async with session.get(url) as response:
            self.index_counter += 1
            print(self.index_counter)
            try:
                results = await response.json()
            except:
                return "AUTH_FAILURE"
            try:
                return results['results'][0]['response']['bbl']
            except:
                return "NO_BBL"

    async def decide_result(self, session, index, row): 
        if self.overwrite or len(self.df.loc[index,"BBL"])==0:
            self.df.loc[index,"BBL"] = await self.get_result(session, f"{row['Building Number']} {row['Street']} {row['City']}")
            # self.df.loc[index,"BBL"] = await self.get_result(session, f"{row['Building Number']} {row['Street']} {row['Zip']}")
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
            self.increment_global_key()

        return self.df