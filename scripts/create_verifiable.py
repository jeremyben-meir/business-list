import pickle
from common import DirectoryFields
import boto3
import csv
import pandas as pd

class CreateVerifiable():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        
    def get_pluto(self,year):

        def set_str(bbl):
            try:
                return str(int(bbl))
            except Exception:
                return None

        path = f'pluto/{year}.p'
        pluto_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        pluto_df["bbl"] = pluto_df["bbl"].apply(lambda bbl: set_str(bbl))
        pluto_df = pluto_df[["bbl","comarea","unitstotal","unitsres"]]
        return pluto_df

    def get_temp_set(self,year):
        cur_date = pd.to_datetime(f"01/01/{year}") #TODO edit timestamp
        next_date = pd.to_datetime(f"01/01/{year+1}") #TODO edit timestamp
        temp_set = self.df.copy()
        temp_set = temp_set[(temp_set["Start Date"] >= cur_date) & (temp_set["Start Date"] < next_date)]
        return temp_set

    def convertFile(self):
        get_path = "data/temp/df-timeline-nosubway"
        pluto_df = self.get_pluto(2021)
        year_list = list(range(2010,2022))
        year_list_str = [str(year) for year in year_list]
        df = None

        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(get_path+".p").get()['Body'].read())
        
        unique_bbl_sample = self.df.sample(n=200)["BBL"].unique()
        self.df = self.df[self.df["BBL"].isin(unique_bbl_sample)]
        
        self.df["OSD"] = 1
        self.df["Match?"] = ""
        for col in year_list_str:
            self.df[col] = ""
        self.df["LatLon"] = self.df.apply(lambda row: str(row["Latitude"]) + " " + str(row["Longitude"]),axis=1)

        for year in year_list:

            print(year)

            pluto_df = self.get_pluto(year)
            temp_set = self.get_temp_set(year)

            merged = temp_set.merge(pluto_df, how='left', left_on=['BBL'], right_on=['bbl'])
            merged = merged.loc[:,~merged.columns.duplicated()]
            merged = merged.loc[~merged.index.duplicated(keep='first')]
            merged = merged.reset_index(drop = True)

            del pluto_df
            del temp_set

            merged["comarea"] = merged["comarea"].fillna(-1).astype(int)
            merged["unitstotal"] = merged["unitstotal"].fillna(-1).astype(int)
            merged["unitsres"] = merged["unitsres"].fillna(-1).astype(int)
            bad_lbid_mask = (((merged["unitstotal"] == merged["unitsres"]) & (merged["unitstotal"] != -1) & (merged["unitsres"] != -1)) | (merged["comarea"] == 0))
            merged = merged[~bad_lbid_mask]

            if df is None:
                df = merged
            else:
                df = pd.concat([merged,df],ignore_index=True)
            
            print(len(merged))
            print()

            del merged

        print(df["BBL"].nunique())

        unique_bbl_sample = df["BBL"].unique()[:100]
        df = df[df["BBL"].isin(unique_bbl_sample)]

        print(df["BBL"].nunique())

        df = df[['Name 1', 'Name 2', 'Name 3', 'OSD', 'Match?', 'BBL', 'Address', 'LatLon', 'Start Date', 'End Date'] + year_list_str]
        df = df.sort_values(by=['BBL', 'End Date'])

        df.to_csv(f"{DirectoryFields.S3_PATH}"+"data/temp/verifiable.csv", index=False, quoting=csv.QUOTE_ALL)

if __name__ == "__main__":
    creator = CreateVerifiable()
    creator.convertFile()