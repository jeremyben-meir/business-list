from datetime import date
import pickle
from scripts.file_manager import FileManager
from pandas.tseries.offsets import YearBegin
from common import DirectoryFields
import pandas as pd
import csv
import boto3


class DateLocObservations():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        
        for year in range(2010,2022):
            path = f"pluto/{year}/"
            df_list = list()
            summary_list = [obj.key for obj in self.s3.Bucket(DirectoryFields.S3_PATH_NAME).objects.filter(Prefix=path) if (obj.key != path and "/." not in obj.key and (".csv" in obj.key or ".txt" in obj.key))]
            if len(summary_list) > 0:
                if len(summary_list) > 1:
                    for key_val in summary_list:
                        df = pd.read_csv(f"{DirectoryFields.S3_PATH}{key_val}", sep=",",low_memory=False)
                        df_list.append(df)
                    res_df = pd.concat(df_list)
                else:
                    res_df = pd.read_csv(f"{DirectoryFields.S3_PATH}{summary_list[0]}", sep=",",low_memory=False)
                res_df = self.apply_trans(res_df, year)
                print(year)
                print(res_df.columns.tolist())
                self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"{path}pluto{year}.p", Body=pickle.dumps(res_df))

        # path = f"pluto/"
        # for obj in self.s3.Bucket(DirectoryFields.S3_PATH_NAME).objects.filter(Prefix=path):
        #     if (obj.key != path and "/." not in obj.key and ".p" in obj.key and ".pdf" not in obj.key):
        #         df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(obj.key).get()['Body'].read())
        #         # print(df)
        #         df["BBL"] = df.apply(lambda row: self.get_bbl(row),axis=1)
        #         print(df["BBL"])
        #         self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=obj.key, Body=pickle.dumps(df))

    def apply_trans(self, res_df, year):
        res_df["Year"] = year
        res_df = res_df.reset_index(drop=True)
        res_df.columns = res_df.columns.str.lower()
        return res_df

    def generate(self):
        pass

if __name__ == "__main__":
    date_loc_observations = DateLocObservations()
    date_loc_observations.generate()