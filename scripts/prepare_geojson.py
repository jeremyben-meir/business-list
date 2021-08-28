import csv, json
from geojson import Feature, FeatureCollection, Point
from common import DirectoryFields
import pandas as pd

features = []
with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/temp/timeline.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for name, llid, address, mindate, maxdate, longitude, latitude in reader:
        latitude, longitude = map(float, (latitude, longitude))
        features.append(
            Feature(
                geometry = Point((longitude, latitude)),
                properties = {
                    'Name': name,
                    'LLID': llid,
                    'Address': address,
                    'Start Date': mindate,
                    'End Date': maxdate,
                    'Duration': int((maxdate-mindate).days)
                }
            )
        )

collection = FeatureCollection(features)
with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/temp/timeline.json', "w") as f:
    f.write('%s' % collection)