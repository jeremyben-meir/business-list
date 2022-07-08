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
        self.df["Max Businesses"] = 0
        print(len(self.df))

    def get_nypd(self,year):
        try:
            return self.nypd_list[year]
        except:
            path = f"subway/source/DOITT_SUBWAY_ENTRANCE_01_13SEPT2010.csv"
            df = pd.read_csv(f"{DirectoryFields.S3_PATH}{path}", sep=",",low_memory=False)
    
    def get_pluto(self,year):
        path = f'pluto/{year}.p'
        pluto_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        return pluto_df
    
    def get_comptroller(self):        
        path = f'comptroller/key_indicators.p'
        comptroller_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        return comptroller_df

    def get_temp_set(self,year,surv):
        cur_date = pd.to_datetime(f"01/01/{year}") #TODO edit timestamp
        if not surv:
            # one_obs = (self.df["End Date"].dt.year == self.df["Start Date"].dt.year) & (self.df["Start Date"].dt.year == year)
            # print(len(self.df[one_obs]))
            # temp_set = self.df[span_date|one_obs] # TODO WHY ARE THERE ONLY ONE OBS
            temp_set = self.df.copy()
            temp_set = temp_set[(temp_set["Start Date"] <= cur_date) & (temp_set["End Date"] >= cur_date)] # TODO WHY ARE THERE ONLY ONE OBS
            temp_set["Months Active"] = (cur_date - self.df["Start Date"]).astype('timedelta64[M]').astype(float).astype(int)
        else:
            temp_set = self.df[(self.df["Start Date"] < cur_date)]
            temp_set.loc[(self.df["End Date"] >= cur_date), "Status"] = False
            temp_set.loc[(self.df["End Date"] >= cur_date), "Months Active"] = (cur_date - self.df["Start Date"]).astype('timedelta64[M]').astype(float).astype(int)
            temp_set.loc[(self.df["End Date"] < cur_date), "Months Active"] = (self.df["End Date"] - self.df["Start Date"]).astype('timedelta64[M]').astype(float).astype(int)
        temp_set['Year'] = year

        return temp_set

    def get_df(self, surv = False):
        self.df = self.df[~self.df["Start Date"].isna()]

        year_list = list(range(2010,2022))
        df = None
        lbid_to_elim = set()
        comptroller_df = self.get_comptroller()

        for year in year_list:
            
            print(year)

            pluto_df = self.get_pluto(year)

            temp_set = self.get_temp_set(year,surv)

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

        # totlen = df["BBL"].nunique()
        # tick = 0
        # grouped = df.groupby("BBL")
        # for name, group in grouped:
        #     max_llid = int(group['Year'].value_counts().max())
        #     # df.loc[df["BBL"]==name,"Max Businesses"] = max_llid
        #     # for year in year_list:
        #     #     df.loc[(df["BBL"]==name)&(df["Year"]==year),"Current Vacancy"] = round(float(len(group[(group["Year"]==year)]))/max_llid,2)
        #     #     df.loc[(df["BBL"]==name)&(df["Year"]==year),"Historical Vacancy"] = round(float(len(group[(group["Year"]<=year)]))/(max_llid*(year-year_list[0]+1)),2)
        #     tick += 1
        #     print(f"{tick} / {totlen}")
        # # df["Current Vacancy"] = df.apply(lambda row: round(float(len(df[(df["Year"]==row["Year"])&(df["BBL"]==row["BBL"])]))/row["Max Businesses"],2),axis=1)
        # # df["Historical Vacancy"] = df.apply(lambda row: round(float(len(df[(df["Year"]<=row["Year"])&(df["BBL"]==row["BBL"])]))/(row["Max Businesses"]*(row["Year"]-df["Year"]+1)),2),axis=1)
        
        df = df.reset_index(drop=True)
        print(len(df))
        return df

    def classifier_models(self):
        df = self.get_df(surv=False)
       
        df["Survive"] = 1
        grouped = df.groupby("LLID")
        totlen = df["LLID"].nunique()
        ticker = 0
        for _, group in grouped:
            df.loc[group['Year'].idxmax(),"Survive"] = 0
            ticker+=1
            print(f'{ticker} / {totlen}',end="\r")
        
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-observations.p', Body=pickle.dumps(df))

    def survival_models(self):
        self.df["Status"] = True
        df = self.get_df(surv=True)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-observations-cox.p', Body=pickle.dumps(df))

if __name__ == "__main__":
    business_observations = BusinessObservations()
    # business_observations.classifier_models()
    business_observations.survival_models()


# mask_2021 = (self.df["Start Date"] <= cur_date) & (self.df["End Date"] >= cur_date)
# self.df.loc[mask_2021,"End Date"] = cur_date
# self.df.loc[mask_2021,"Status"] = False
# self.df["Months Active"] = (self.df["End Date"] - self.df["Start Date"]).astype('timedelta64[M]').astype(float).astype(int)
# self.df['Year'] = pd.DatetimeIndex(self.df['Start Date']).year
# print(self.df[self.df["End Date"].dt.year == 2020])