import csv, json
from geojson import Feature, FeatureCollection, Point
from common import DirectoryFields

features = []
with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/temp/merged.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for latitude, longitude, weather, temp in reader:
        latitude, longitude = map(float, (latitude, longitude))
        features.append(
            Feature(
                geometry = Point((longitude, latitude)),
                properties = {
                    'weather': weather,
                    'temp': temp
                }
            )
        )

collection = FeatureCollection(features)
with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/temp/merged.csv', "w") as f:
    f.write('%s' % collection)