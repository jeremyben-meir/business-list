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
        self.obs_df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-observations.p").get()['Body'].read())
        self.obs_df['Start Date'] = pd.to_datetime(self.obs_df['Start Date']).dt.date #TODO: check
        self.obs_df['End Date'] = pd.to_datetime(self.obs_df['End Date']).dt.date #TODO: check
        self.year_list = list(range(2010,2022)) # TODO: CHANGE LOCIC

    @staticmethod
    def create_llid_json(df,geotype="LLID"):
        features = list()
        print(len(df))
        totlen = df["BBL"].nunique()
        ticker = 0
        grouped = df.groupby("BBL")
        for name, group in grouped:
            num_vals = len(group)
            lon = group["Longitude"].astype(float).max()
            lat = group["Latitude"].astype(float).max()
            counter = 0
            for index, row in group.iterrows():
                duration = (row["End Date"] - row["Start Date"]).days
                duration =  str(math.floor(duration/365.0) if not pd.isnull(duration) else 0)
                
                if num_vals == 1:
                    latitude, longitude = map(float, (lat, lon))
                else:
                    degree_val = 360.0*(counter/num_vals)
                    rand_radian = math.radians(degree_val)
                    latitude, longitude = map(float, (lat+(math.sin(rand_radian)/15000.0), lon+(math.cos(rand_radian)/12000.0)))
                
                counter += 1

                features.append(
                    Feature(
                        geometry = Point((longitude, latitude)),
                        properties = {
                            'Name': row["Name"],
                            'LLID': row["LLID"],
                            'Address': row["Address"],
                            # 'NAICS': str(row["NAICS"])[0],
                            # 'NAICS Title': str(row["NAICS Title"]),
                            'Contact Phone': str(row["Contact Phone"]),
                            "Duration": duration,
                            'Start Date': str(row["Start Date"].strftime('%Y-%m-%d')),
                            'End Date': str(row["End Date"].strftime('%Y-%m-%d')),
                            "color": duration if geotype == "LLID" else str(1-row["Survive"]),
                        }
                    )
                )
            ticker += 1
            print(f" {ticker} / {totlen}",end="\r")
        print(f"\n{geotype}s complete")
        return FeatureCollection(features)
    
    def create_bbl_json_2(self):
        features = list()
        df = self.obs_df
        totlen = df["BBL"].nunique()
        ticker = 0
        grouped = df.groupby("BBL")
        for name, group in grouped:
            latitude, longitude = map(float, (group["Latitude"].max(), group["Longitude"].max()))
            ## VACANCY
            max_llid = int(group['Year'].value_counts().max())
            vacancy = round(float((len(group) / len(self.year_list)) / max_llid),2)

            features.append(
                Feature(
                    geometry = Point((longitude, latitude)),
                    properties = {
                        'BBL': name,
                        'Vacancy': vacancy,
                        'Max Business': max_llid,
                        'color': str(math.floor(vacancy*10.0))[0],
                    }
                )
            )

            ticker += 1
            print(f" {ticker} / {totlen}",end="\r")

        print("\nBBLs complete")
        collection = FeatureCollection(features)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="data/geo/bbl_timeline.json", Body=('%s' % collection))
    
    def start_geo(self):
        # LLID JSON
        df = self.obs_df[self.obs_df["Year"] == 2021]
        collection = self.create_llid_json(df)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="data/geo/llid_timeline.json", Body=('%s' % collection))

        # BBL JSON
        self.create_bbl_json_2()        

if __name__ == "__main__":
    prepare_geojson = PrepareGeojson()
    prepare_geojson.start_geo()



    # def num_llids_for_date(self,df,date):
    #     return len(df[(df["Start Date"]<=date) & (df["End Date"]>=date)])

    # def create_bbl_json(self):
    #     features = list()
    #     df = self.bbl_df
    #     totlen = df["BBL"].nunique()
    #     ticker = 0
    #     for bbl_val in df["BBL"].unique():
    #         bbl_df = df[df["BBL"] == bbl_val]
    #         latitude, longitude = map(float, (bbl_df["Latitude"].max(), bbl_df["Longitude"].max()))
            
    #         ## VACANCY
    #         max_llid = max([self.num_llids_for_date(bbl_df,date) for date in bbl_df["Start Date"].unique()])
    #         date = pd.to_datetime('20100101', format='%Y%m%d', errors='ignore')
    #         vacancy_total = list()
    #         if max_llid == 0:
    #             max_llid = 1
    #         while date < pd.to_datetime("today"):
    #             vacancy_total.append(self.num_llids_for_date(bbl_df,date) / max_llid)
    #             date += pd.Timedelta(days=90)

    #         ## TURNOVER
    #         date = pd.to_datetime('20100201', format='%Y%m%d', errors='ignore')
    #         llid_date_list = list()
    #         turnover_total = list()

    #         while date < pd.to_datetime("today"):
    #             llid_date_list.append(self.num_llids_for_date(bbl_df,date))
    #             date += pd.Timedelta(days=365)

    #         for num in range(len(llid_date_list)-1):
    #             turnover_total.append((llid_date_list[num+1] - llid_date_list[num]) / (1 if llid_date_list[num] == 0 else llid_date_list[num]))

    #         vacancy = 1.0 - (sum(vacancy_total) / len(vacancy_total))
    #         turnover = (sum(turnover_total) / len(turnover_total))
    #         features.append(
    #             Feature(
    #                 geometry = Point((longitude, latitude)),
    #                 properties = {
    #                     'BBL': bbl_val,
    #                     'Vacancy': str(math.floor(vacancy*10.0))[0],
    #                     'Turnover': str(math.floor(turnover*10.0))[0],
    #                     'vacancy': vacancy,
    #                     'turnover': turnover,
    #                     'Max Business': max_llid
    #                 }
    #             )
    #         )

    #         ticker += 1
    #         print(f" {ticker} / {totlen}",end="\r")
    #     print("\nBBLs complete")
    #     collection = FeatureCollection(features)
    #     self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="data/bbl_timeline.json", Body=('%s' % collection))
    