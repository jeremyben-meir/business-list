import csv, json
from types import prepare_class
from geojson import Feature, FeatureCollection, Point
from common import DirectoryFields
import pandas as pd
import pickle

class PrepareGeojson():

    def __init__(self):
        self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-timeline_lid.p", "rb" ))
        self.features = list()
    
    def create_json(self):
        for _, row in self.df.iterrows():
            latitude, longitude = map(float, (row["Latitude"], row["Longitude"]))
            self.features.append(
                Feature(
                    geometry = Point((longitude, latitude)),
                    properties = {
                        'Name': row["Name"],
                        'LLID': row["LLID"],
                        'Address': row["Address"],
                        'Industry': str(row["Industry"]),
                        'Start Date': str(row["Start Date"]),
                        'End Date': str(row["End Date"])
                    }
                )
            )
        collection = FeatureCollection(self.features)
        with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/timeline.json', "w") as f:
            f.write('%s' % collection)

if __name__ == "__main__":
    prepare_geojson = PrepareGeojson()
    prepare_geojson.create_json()



