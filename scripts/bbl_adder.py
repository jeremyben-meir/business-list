from scripts.progress_meter import ProgressMeter
import pandas as pd
from geosupport import Geosupport
import os

class BBLAdder:

    def get_df(self):
        return self.df

    def __init__(self, df):

        # os.environ["GEOFILES"] = "https://locus-data.s3.us-east-1.amazonaws.com/version-21c_21.3/fls/"
        # os.environ["LD_LIBRARY_PATH"] = "https://locus-data.s3.us-east-1.amazonaws.com/version-21c_21.3/lib/"

        self.g = Geosupport()
        self.df = df
        self.progress_meter = ProgressMeter(len(self.df), precision=1)
        
            
    def get_result(self, input_dict, bbl_input=False):

        try:
            results = self.g.bbl(input_dict) if bbl_input else self.g.address(input_dict)
        except:
            return "NO_BBL", "NO_ZIP", 0.0, 0.0, 
            
        try:
            bbl = results['BOROUGH BLOCK LOT (BBL)']['BOROUGH BLOCK LOT (BBL)']
            bbl = "NO_BBL" if bbl == "0" else bbl
        except:
            bbl = "NO_BBL"
            
        try:
            zipcode = results["ZIP Code"]
            zipcode = "NO_ZIP" if zipcode == "0" else zipcode
        except:
            zipcode = "NO_ZIP"

        try:
            longitude = results['Longitude']
            latitude = results['Latitude']
        except:
            longitude = 0.0
            latitude = 0.0
        
        return bbl, zipcode, longitude, latitude

    def decide_result(self, row, city_zip):
        bbl_valid = len(row["BBL"]) == 10 and row['BBL'] != "0000000000" and row["BBL"].isdigit()
        lon_lat_valid = row["Longitude"] != 0.0 and row["Latitude"] != 0.0

        if not (bbl_valid and lon_lat_valid):
            if bbl_valid:
                input_dict = {"bbl":row['BBL']}
                bbl, zipcode, longitude, latitude = self.get_result(input_dict, bbl_input=True)
            else:
                input_dict = {
                    "Borough Code-1":row[city_zip],
                    "Street Name-1":f"{row['Building Number']} {row['Street']}",
                }
                bbl, zipcode, longitude, latitude = self.get_result(input_dict)
            row["BBL"] = bbl
            row["Zip"] = zipcode
            row["Longitude"] = longitude
            row["Latitude"] = latitude
            
        
        self.progress_meter.tick()
        return row

    def add_bbl_starter(self):  # input row must have headers 'Building Number,' 'Street,' 'City,' 'Zip,' 'BBL'
        
        self.df["Longitude"] = 0.0
        self.df["Latitude"] = 0.0
        
        for city_zip in ["Zip","City"]:

            self.df = self.df.apply(lambda row : self.decide_result(row, city_zip),axis=1)
            self.progress_meter.ticker = 0

        return self.df