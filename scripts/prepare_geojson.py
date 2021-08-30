import csv, json
from types import prepare_class
from geojson import Feature, FeatureCollection, Point
from common import DirectoryFields
import pandas as pd
import pickle
import math
import random

class PrepareGeojson():

    def __init__(self):
        self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-timeline.p", "rb" ))
        self.features = list()
    
    def create_llid_json(self):
        df = self.df
        df = df[pd.to_datetime("today") + pd.Timedelta(days=-60) < df["End Date"]]
        for bbl_val in df["BBL"].unique():
            bbl_df = df[df["BBL"] == bbl_val]
            num_vals = len(bbl_df)
            ticker = 0.0
            lon = bbl_df["Longitude"].max()
            lat = bbl_df["Latitude"].max()
            for _,row in bbl_df.iterrows():
                duration = (row["End Date"] - row["Start Date"]).days
                duration =  str(math.floor(duration/365.0) if not pd.isnull(duration) else 0)
                # duration =  ""
                if num_vals == 1:
                    latitude, longitude = map(float, (lat, lon))
                else:
                    degree_val = 360.0*(ticker/num_vals)
                    rand_radian = math.radians(degree_val)
                    latitude, longitude = map(float, (lat+(math.sin(rand_radian)/15000.0), lon+(math.cos(rand_radian)/12000.0)))
                self.features.append(
                    Feature(
                        geometry = Point((longitude, latitude)),
                        properties = {
                            'Name': row["Name"],
                            'LLID': row["LLID"],
                            'Address': row["Address"],
                            'NAICS': str(row["NAICS"])[0],
                            'NAICS Title': str(row["NAICS Title"]),
                            'Contact Phone': str(row["Contact Phone"]),
                            "Duration": duration,
                            'Start Date': str(row["Start Date"]),
                            'End Date': str(row["End Date"])
                        }
                    )
                )
                ticker += 1.0
        collection = FeatureCollection(self.features)
        with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/llid_timeline.json', "w") as f:
            f.write('%s' % collection)

if __name__ == "__main__":
    prepare_geojson = PrepareGeojson()
    prepare_geojson.create_llid_json()



