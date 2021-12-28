from datetime import date
import pickle
from decimal import Decimal
from pandas.tseries.offsets import YearBegin
from common import DirectoryFields
import pandas as pd
import csv
import boto3
import math

class BusinessObservations():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-timeline.p").get()['Body'].read())
        print(len(self.df))
    
    def get_pluto(self,year):
        path = f'pluto/{year}.p'
        pluto_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        return pluto_df
    
    def get_comptroller(self):        
        path = f'comptroller/key_indicators.p'
        comptroller_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        return comptroller_df

    def generate(self):
        year_list = list(range(2010,2022))
        df = None
        lbid_to_elim = set()

        comptroller_df = self.get_comptroller()

        for year in year_list:
            
            print(year)

            pluto_df = self.get_pluto(year)

            month = '01'
            cur_date = pd.to_datetime(f"{month}/01/{year}") #TODO edit timestamp
            span_date = (self.df["Start Date"] <= cur_date) & (self.df["End Date"] >= cur_date)
            one_obs = (self.df["End Date"].dt.year == self.df["Start Date"].dt.year) & (self.df["Start Date"].dt.year == year)
            temp_set = self.df[span_date|one_obs] # TODO WHY ARE THERE ONLY ONE OBS
            temp_set["Observed"] = cur_date #TODO CHECK
            temp_set["Year"] = year #TODO CHECK

            def set_str(bbl):
                try:
                    return str(int(bbl))
                except Exception:
                    return None
            pluto_df["bbl"] = pluto_df["bbl"].apply(lambda bbl: set_str(bbl))
            merged = temp_set.merge(pluto_df, how='left', left_on=['BBL'], right_on=['bbl'])
            merged = merged.merge(comptroller_df, how='left', left_on=['Year'], right_on=['year'])
            merged = merged.loc[:,~merged.columns.duplicated()]
            merged = merged.loc[~merged.index.duplicated(keep='first')]
            merged = merged.reset_index(drop = True)

            # print(merged["Year"])
            # print(comptroller_df["year"])

            del pluto_df
            del temp_set

            merged["comarea"] = merged["comarea"].fillna(-1).astype(int)
            merged["unitstotal"] = merged["unitstotal"].fillna(-1).astype(int)
            merged["unitsres"] = merged["unitsres"].fillna(-1).astype(int)
            bad_lbid_mask = (((merged["unitstotal"] == merged["unitsres"]) & (merged["unitstotal"] != -1) & (merged["unitsres"] != -1)) | (merged["comarea"] == 0))
            bad_lbid_df = merged[bad_lbid_mask]
            bad_lbid = bad_lbid_df["LBID"].unique().tolist()
            lbid_to_elim.update(bad_lbid)
            merged = merged[~bad_lbid_mask]

            if df is None:
                df = merged
            else:
                df = pd.concat([merged,df],ignore_index=True)

            print(len(merged))
            print()

            del merged
       
        # self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-bbls.p', Body=pickle.dumps(self.df[~self.df["LBID"].isin(lbid_to_elim)]))

        df = df[~df["LBID"].isin(lbid_to_elim)]
        print(len(df))
        df["Survive"] = 1
        df = df.reset_index(drop=True)
        grouped = df.groupby("LLID")
        totlen = df["LLID"].nunique()
        ticker = 0
        for _, group in grouped:
            df.loc[group['Observed'].idxmax(),"Survive"] = 0
            ticker+=1
            print(f'{ticker} / {totlen}',end="\r")
        # df = df[df["Observed"] < pd.to_datetime(f"01/01/2021")]
        df["Months Active"] = (df["Observed"] - df["Start Date"]).astype('timedelta64[M]').astype(int)
        
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-observations.p', Body=pickle.dumps(df))

    def generate_cox(self):
        cur_date = pd.to_datetime(f"01/01/2021") #TODO edit timestamp
        self.df = self.df[~self.df["Start Date"].isna()] # TODO: why are there NAN
        self.df["Status"] = True
        mask_2021 = (self.df["Start Date"] <= cur_date) & (self.df["End Date"] >= cur_date)
        self.df.loc[mask_2021,"End Date"] = cur_date
        self.df.loc[mask_2021,"Status"] = False
        self.df["Months Active"] = (self.df["End Date"] - self.df["Start Date"]).astype('timedelta64[M]').astype(float).astype(int)
        self.df['Year'] = pd.DatetimeIndex(self.df['Start Date']).year
        # print(self.df[self.df["End Date"].dt.year == 2020])

        year_list = list(range(2010,2021))
        df = None
        lbid_to_elim = set()

        comptroller_df = self.get_comptroller()

        for year in year_list:
            
            print(year)

            pluto_df = self.get_pluto(year)

            cur_date = pd.to_datetime(f"01/01/{year}") #TODO edit timestamp
            temp_set = self.df[self.df["Year"] == year]

            def set_str(bbl):
                try:
                    return str(int(bbl))
                except Exception:
                    return None
            pluto_df["bbl"] = pluto_df["bbl"].apply(lambda bbl: set_str(bbl))
            merged = temp_set.merge(pluto_df, how='left', left_on=['BBL'], right_on=['bbl'])
            merged = merged.merge(comptroller_df, how='left', left_on=['Year'], right_on=['year'])
            merged = merged.loc[:,~merged.columns.duplicated()]
            merged = merged.loc[~merged.index.duplicated(keep='first')]
            merged = merged.reset_index(drop = True)

            del pluto_df
            del temp_set

            merged["comarea"] = merged["comarea"].fillna(-1).astype(int)
            merged["unitstotal"] = merged["unitstotal"].fillna(-1).astype(int)
            merged["unitsres"] = merged["unitsres"].fillna(-1).astype(int)
            bad_lbid_mask = (((merged["unitstotal"] == merged["unitsres"]) & (merged["unitstotal"] != -1) & (merged["unitsres"] != -1)) | (merged["comarea"] == 0))
            bad_lbid_df = merged[bad_lbid_mask]
            bad_lbid = bad_lbid_df["LBID"].unique().tolist()
            lbid_to_elim.update(bad_lbid)
            merged = merged[~bad_lbid_mask]

            if df is None:
                df = merged
            else:
                df = pd.concat([merged,df],ignore_index=True)

            print(len(merged))
            print()

            del merged

        df = df[~df["LBID"].isin(lbid_to_elim)]
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-observations-cox.p', Body=pickle.dumps(df))

if __name__ == "__main__":
    business_observations = BusinessObservations()
    # business_observations.generate()
    business_observations.generate_cox()