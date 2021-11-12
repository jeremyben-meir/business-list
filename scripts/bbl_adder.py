from scripts.progress_meter import ProgressMeter
import json
import pandas as pd
import asyncio
import aiohttp
import concurrent.futures
from geosupport import Geosupport

class BBLAdder:

    def get_df(self):
        return self.df

    def __init__(self, df, keylist):
        self.g = Geosupport()
        self.df = df
        self.segment_size = 2450
        self.progress_meter = ProgressMeter(len(self.df), precision=1)
            
    async def get_result(self, input_dict, bbl_input=False):

        try:
            results = self.g.bbl(input_dict) if bbl_input else self.g.address(input_dict)
        except:
            return "NO_BBL", 0.0, 0.0
        
        try:
            bbl = results['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']
            bbl = "NO_BBL" if bbl == "0" else bbl
        except:
            bbl = "NO_BBL"

        try:
            longitude = results['Longitude']
            latitude = results['Latitude']
        except:
            longitude = 0.0
            latitude = 0.0
        
        return bbl, longitude, latitude

    async def decide_result(self, index, row, city_zip):
        bbl_valid = len(row["BBL"]) == 10 and row['BBL'] != "0000000000" and row["BBL"].isdigit()
        lon_lat_valid = row["Longitude"] != 0.0 and row["Latitude"] != 0.0

        if not (bbl_valid and lon_lat_valid):
            if bbl_valid:
                input_dict = {"bbl":row['BBL']}
                bbl, longitude, latitude = await self.get_result(input_dict, bbl_input=True)
            else:
                input_dict = {
                    "Borough Code-1":row[city_zip],
                    "Street Name-1":f"{row['Building Number']} {row['Street']}",
                }
                bbl, longitude, latitude = await self.get_result(input_dict)
            print(bbl)
            print(longitude)
            print(latitude)
            self.df.loc[index,"BBL"] = bbl
            self.df.loc[index,"Longitude"] = longitude
            self.df.loc[index,"Latitude"] = latitude
        
        self.progress_meter.tick()

    async def add_bbl_helper(self,top_index,bot_index,city_zip):
        tasks = []
        start_index = top_index
        while top_index <= bot_index and top_index - start_index < self.segment_size:
            task = asyncio.ensure_future(self.decide_result(top_index,self.df.loc[top_index],city_zip))
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
            
            self.progress_meter.ticker = 0

        return self.df