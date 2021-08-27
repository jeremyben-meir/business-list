from scripts.progress_meter import ProgressMeter
import json
import pandas as pd
import asyncio
import aiohttp
import concurrent.futures

class BBLAdder:

    def get_df(self):
        return self.df

    def __init__(self, df, keylist):
        self.keylist = keylist
        self.key_row = 0
        self.app_key = self.keylist[self.key_row][0]
        self.app_id = self.keylist[self.key_row][1]
        self.df = df
        self.segment_size = 2450
        self.progress_meter = ProgressMeter(len(self.df), precision=1)

    def increment_global_key(self):
        self.key_row += 1
        if self.key_row >= len(self.keylist):
            self.key_row = 0
        self.app_key = self.keylist[self.key_row][0]
        self.app_id = self.keylist[self.key_row][1]
            
    async def get_result(self, session, url):
        async with session.get(url) as response:
            
            try:
                results = await response.json()
            except:
                return "AUTH_FAILURE", "AUTH_FAILURE", "AUTH_FAILURE"
            
            try:
                bbl = results['results'][0]['response']['bbl']
                bbl = "NO_BBL" if bbl == "0" else bbl
            except:
                bbl = "NO_BBL"

            try:
                longitude = results['results'][0]['response']['longitudeInternalLabel']
                latitude = results['results'][0]['response']['latitudeInternalLabel']
            except:
                longitude = 0.0
                latitude = 0.0
            
            return bbl, longitude, latitude

    async def decide_result(self, session, index, row, city_zip):
        bbl_valid = len(row["BBL"]) == 10 and row['BBL'] != "0000000000" and row["BBL"].isdigit()
        lon_lat_valid = row["Longitude"] != 0.0 and row["Latitude"] != 0.0

        if not (bbl_valid and lon_lat_valid):
            if bbl_valid:
                url = f"https://api.cityofnewyork.us/geoclient/v1/search.json?input={row['BBL']}&app_id={self.app_id}&app_key={self.app_key}"
            else:
                url = f"https://api.cityofnewyork.us/geoclient/v1/search.json?input={row['Building Number']} {row['Street']} {row[city_zip]}&app_id={self.app_id}&app_key={self.app_key}"
            bbl, longitude, latitude = await self.get_result(session, url)
            self.df.loc[index,"BBL"] = bbl
            self.df.loc[index,"Longitude"] = longitude
            self.df.loc[index,"Latitude"] = latitude
        
        self.progress_meter.tick()

    async def add_bbl_helper(self,top_index,bot_index,city_zip):
        async with aiohttp.ClientSession() as session:
            tasks = []
            start_index = top_index
            while top_index <= bot_index and top_index - start_index < self.segment_size:
                task = asyncio.ensure_future(self.decide_result(session,top_index,self.df.loc[top_index],city_zip))
                tasks.append(task)
                top_index+=1
            await asyncio.gather(*tasks)

    def add_bbl_starter(self):  # input row must have headers 'Building Number,' 'Street,' 'City,' 'Zip,' 'BBL'
        
        self.df["Longitude"] = 0.0
        self.df["Latitude"] = 0.0
        
        for city_zip in ["City","Zip"]:

            if len(self.df) > 0:
                top_index = self.df.iloc[0].name
                bot_index = self.df.iloc[-1].name
                    
                while top_index <= bot_index:
                    asyncio.run(self.add_bbl_helper(top_index,bot_index,city_zip))
                    top_index+=self.segment_size
                    self.increment_global_key()
            
            self.progress_meter.ticker = 0

        return self.df