from scripts.progress_meter import ProgressMeter
import json
import pandas as pd
import asyncio
import aiohttp
import concurrent.futures

class BBLAdder:

    def get_df(self):
        return self.df

    def __init__(self, df, keylist,overwrite, id_):
        self.keylist = keylist
        self.key_row = 0
        self.app_key = self.keylist[self.key_row][0]
        self.app_id = self.keylist[self.key_row][1]
        self.df = df
        self.overwrite = overwrite
        self.segment_size = 2450
        self.index_counter = 0
        self.id = id_

        if self.overwrite == False:

            self.org_df = df[~(df["BBL"].eq("AUTH_FAILURE") | df["BBL"].eq("NO_BBL"))]
            self.df = df[df["BBL"].eq("AUTH_FAILURE") | df["BBL"].eq("NO_BBL")]
            self.df = self.df.reset_index(drop=True)

        if self.id == 0:
            self.progress_meter = ProgressMeter(len(self.df), precision=1)

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
            if self.id == 1:
                print(self.index_counter, end="\r")
            try:
                results = await response.json()
            except:
                return "AUTH_FAILURE"
            try:
                return results['results'][0]['response']['bbl']
            except:
                return "NO_BBL"

    async def decide_result(self, session, index, row): 
        self.df.loc[index,"BBL"] = await self.get_result(session, f"{row['Building Number']} {row['Street']} {row['Zip'] if self.overwrite else row['City']}")
        if self.id == 0:
            self.progress_meter.tick()

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
        if len(self.df) > 0:
            top_index = self.df.iloc[0].name
            bot_index = self.df.iloc[-1].name
                
            while top_index <= bot_index:
                asyncio.run(self.add_bbl_helper(top_index,bot_index))
                top_index+=self.segment_size
                self.increment_global_key()

        if self.overwrite == False:
            return pd.concat([self.org_df,self.df], ignore_index=True)
        return self.df