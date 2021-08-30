import csv, json
from types import prepare_class
from geojson import Feature, FeatureCollection, Point
from common import DirectoryFields
import pandas as pd
import pickle

class PrepareGeojson():

    def __init__(self):
        self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-timeline.p", "rb" ))
        self.features = list()
    
    def create_llid_json(self):
        df = self.df
        df = df[pd.to_datetime("today") + pd.Timedelta(days=-60) < df["End Date"]]
        for _, row in df.iterrows():
            latitude, longitude = map(float, (row["Latitude"], row["Longitude"]))
            self.features.append(
                Feature(
                    geometry = Point((longitude, latitude)),
                    properties = {
                        'Name': row["Name"],
                        'LLID': row["LLID"],
                        'Address': row["Address"],
                        'NAICS': str(row["NAICS"]),
                        'NAICS Title': str(row["NAICS Title"]),
                        'Contact Phone': str(row["Contact Phone"]),
                        'Start Date': str(row["Start Date"]),
                        'End Date': str(row["End Date"])
                    }
                )
            )
        collection = FeatureCollection(self.features)
        with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/llid_timeline.json', "w") as f:
            f.write('%s' % collection)

if __name__ == "__main__":
    prepare_geojson = PrepareGeojson()
    prepare_geojson.create_llid_json()



