import csv, json
from types import prepare_class
from geojson import Feature, FeatureCollection, Point
from common import DirectoryFields
import pandas as pd
import pickle
import math
import random
import boto3

class PrepareGeojson():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-timeline.p").get()['Body'].read())
        # self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-timeline.p", "rb" ))
    
    def create_llid_json(self):
        features = list()
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
                features.append(
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
        # collection = FeatureCollection(features)
        # with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/llid_timeline.json', "w") as f:
        #     f.write()
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="/data/llid_timeline.json", Body=('%s' % collection))

    def num_llids_for_date(self,df,date):
        return len(df[(df["Start Date"]<=date) & (df["End Date"]>=date)])
    
    def create_bbl_json(self):
        features = list()
        df = self.df
        for bbl_val in df["BBL"].unique():
            bbl_df = df[df["BBL"] == bbl_val]
            latitude, longitude = map(float, (bbl_df["Latitude"].max(), bbl_df["Longitude"].max()))
            
            ## VACANCY
            max_llid = max([self.num_llids_for_date(bbl_df,date) for date in bbl_df["Start Date"].unique()])
            date = pd.to_datetime('20100101', format='%Y%m%d', errors='ignore')
            vacancy_total = list()
            while date < pd.to_datetime("today"):
                vacancy_total.append(self.num_llids_for_date(bbl_df,date) / max_llid)
                date += pd.Timedelta(days=30)

            ## TURNOVER
            date = pd.to_datetime('20100201', format='%Y%m%d', errors='ignore')
            llid_date_list = list()
            turnover_total = list()
            while date < pd.to_datetime("today"):
                llid_date_list.append(self.num_llids_for_date(bbl_df,date))
                date += pd.Timedelta(days=365)
            for num in range(len(llid_date_list)-1):
                turnover_total.append((llid_date_list[num+1] - llid_date_list[num]) / (1 if llid_date_list[num] == 0 else llid_date_list[num]))

            vacancy = 1.0 - (sum(vacancy_total) / len(vacancy_total))
            turnover = (sum(turnover_total) / len(turnover_total))
            features.append(
                Feature(
                    geometry = Point((longitude, latitude)),
                    properties = {
                        'BBL': bbl_val,
                        'Vacancy': str(math.floor(vacancy*10.0))[0],
                        'Turnover': str(math.floor(turnover*10.0))[0],
                        'vacancy': vacancy,
                        'turnover': turnover,
                        'Max Business': max_llid
                    }
                )
            )
        # collection = FeatureCollection(features)
        # with open(f'{DirectoryFields.LOCAL_LOCUS_PATH}/data/bbl_timeline.json', "w") as f:
        #     f.write('%s' % collection)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="/data/bbl_timeline.json", Body=('%s' % collection))
        

if __name__ == "__main__":
    prepare_geojson = PrepareGeojson()
    prepare_geojson.create_llid_json()
    prepare_geojson.create_bbl_json()



