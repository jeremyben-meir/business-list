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
            
    async def get_result(self, session, url, bbl_input=False):

        for retry_num in range(3):

            try:

                async with session.get(url) as response:
                    
                    try:
                        results = await response.json()
                    except:
                        return "AUTH_FAILURE", "AUTH_FAILURE", "AUTH_FAILURE"

                    try:
                        result_dict = results['bbl'] if bbl_input else results['results'][0]['response']
                    except:
                        return "NO_BBL", 0.0, 0.0
                    
                    try:
                        bbl = result_dict['bbl']
                        bbl = "NO_BBL" if bbl == "0" else bbl
                    except:
                        bbl = "NO_BBL"

                    try:
                        longitude = result_dict['longitudeInternalLabel']
                        latitude = result_dict['latitudeInternalLabel']
                    except:
                        longitude = 0.0
                        latitude = 0.0
                    
                    return bbl, longitude, latitude
                
            except asyncio.TimeoutError:

                print(f"Retry {retry_num}")
        
        return "TIMEOUT_ERROR", "TIMEOUT_ERROR", "TIMEOUT_ERROR"

    async def decide_result(self, session, index, row, city_zip):
        bbl_valid = len(row["BBL"]) == 10 and row['BBL'] != "0000000000" and row["BBL"].isdigit()
        lon_lat_valid = row["Longitude"] != 0.0 and row["Latitude"] != 0.0

        if not (bbl_valid and lon_lat_valid):
            if bbl_valid:
                borough_dict = {'1':'manhattan','2':'bronx','3':'brooklyn','4':'queens','5':'staten island'}
                # url = f"https://api.cityofnewyork.us/geoclient/v1/search.json?input={row['BBL']}&app_id={self.app_id}&app_key={self.app_key}"
                url = f"https://api.cityofnewyork.us/geoclient/v1/bbl.json?borough={borough_dict[row['BBL'][0]]}&block={row['BBL'][1:6]}&lot={row['BBL'][6:]}&app_id={self.app_id}&app_key={self.app_key}"
                bbl, longitude, latitude = await self.get_result(session, url, bbl_input=True)
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