from datetime import date
import pickle

from pandas.tseries.offsets import YearBegin
from common import DirectoryFields
import pandas as pd
import csv
import boto3

class BusinessObservations():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-timeline.p").get()['Body'].read())

    def generate(self):
        # self.df["Observed"] = pd.NaT
        self.df['End Date'] = pd.to_datetime(self.df['End Date']).dt.date
        df = pd.DataFrame()
        for year in range(2010,2022):
            # for month in ["01","06"]:
            month = '01'
            cur_date = pd.to_datetime(f"{month}/01/{year}")
            temp_set = self.df[(self.df["Start Date"] <= cur_date) & (self.df["End Date"] >= cur_date)]
            temp_set["Observed"] = cur_date
            temp_set["Year"] = year
            df = df.append(temp_set)
        
        df["Survive"] = 1
        df = df.reset_index(drop=True)
        grouped = df.groupby("LLID")
        for name, group in grouped:
            df.loc[group['Observed'].idxmax(),"Survive"] = 0
        df = df[df["Observed"] < pd.to_datetime(f"01/01/2021")]
        df["Months Active"] = (df["Observed"] - df["Start Date"]).astype('timedelta64[M]').astype(int)
        
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-business-observations.p', Body=pickle.dumps(df))

        print(df.columns.tolist())


if __name__ == "__main__":
    business_observations = BusinessObservations()
    business_observations.generate()