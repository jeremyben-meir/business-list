from datetime import date
import pickle
from common import DirectoryFields
import pandas as pd
import csv
import boto3
import numpy as np
import geopandas
from thefuzz import fuzz
# from cleanco import cleanco

class CreateTimeline():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-assigned.p").get()['Body'].read())
        # self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-merged.p").get()['Body'].read())
        print(self.df.columns.tolist())

    def generate_subway(self):
        path = f"subway/source/DOITT_SUBWAY_ENTRANCE_01_13SEPT2010.csv"
        df = pd.read_csv(f"{DirectoryFields.S3_PATH}{path}", sep=",",low_memory=False)
        df = df.reset_index(drop=True)
        df["longitude"] = df["the_geom"].apply(lambda cell: float(cell.split(" ")[1][1:]))
        df["latitude"] = df["the_geom"].apply(lambda cell: float(cell.split(" ")[2][:-1]))
        gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.longitude, df.latitude))
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"subway/data.p", Body=pickle.dumps(gdf))
        return gdf

    def get_lim_date_from_cols(self, curgrp,col_list,is_maximum):
        lim_date = [(curgrp[date].max() if is_maximum else curgrp[date].min()) for date in col_list]
        lim_list = [date for date in lim_date if not pd.isnull(date)]
        if len(lim_list) == 0:
            return pd.NaT
        return max(lim_list) if is_maximum else min(lim_list)

    def get_max_end(self, curgrp):
        insp_res = (curgrp["INSP Result"].max() == "Out of Business")
        app_res = (curgrp["APP Status"].max() == "OOB")

        # lims = self.get_lim_date_from_cols(curgrp,date_list,True)
        # if lims and lims < maxdate:
        #     maxdate = pd.Timedelta(days=550) + lims

        maxdate_list = list()
        date_list = ["CHRG Date","INSP Date","Last INSP Date","Case Dec. Date","Temp Op Letter Expiration","APP End Date"]
        maxdate_list.append(self.get_lim_date_from_cols(curgrp,date_list,True) + pd.Timedelta(days=550))
        date_list = ['APP Status Date', 'APP Start Date', 'LIC Start Date', 'Temp Op Letter Issued', 'LIC Issue Date', 'Grade Date', 'LIC Filing Date']
        maxdate_list.append(self.get_lim_date_from_cols(curgrp,date_list,True) + pd.Timedelta(days=365))
        date_list = ["LIC Exp Date","RSS Date"]
        maxdate_list.append(self.get_lim_date_from_cols(curgrp,date_list,True))
        date_list = ["Out of Business Date"]
        if insp_res:
            date_list.append("INSP Date")
        if app_res:
            date_list.append("APP Status")
        maxdate_list.append(self.get_lim_date_from_cols(curgrp,date_list,False))
        maxdate_list = [elt for elt in maxdate_list if not pd.isnull(elt)]

        all_col = ['CHRG Date', 'INSP Date', 'APP Status Date', 'APP Start Date', 'APP End Date', 'LIC Start Date', 'LIC Exp Date', 'Temp Op Letter Issued', 'RSS Date', 'Last INSP Date', 'Out of Business Date', 'LIC Issue Date', 'Grade Date', 'Case Dec. Date', 'LIC Filing Date']
        if maxdate_list:
            return_val = max(self.get_lim_date_from_cols(curgrp,all_col,False) , min(maxdate_list))
        else:
            return_val = self.get_lim_date_from_cols(curgrp,all_col,False) 
        return return_val if return_val < pd.to_datetime("today") else pd.to_datetime("today")

    def type_cast(self, df, all_date_list):
        df["Business Name"] = df["Business Name"].fillna("")
        df["Business Name 2"] = df["Business Name 2"].fillna("")
        df["Business Name 3"] = df["Business Name 3"].fillna("")
        df["Building Number"] = df["Building Number"].fillna("")
        df["Contact Phone"] = df["Contact Phone"].fillna("")
        df["INSP Result"] = df["INSP Result"].fillna("")
        df["APP Status"] = df["APP Status"].fillna("")
        df["Street"] = df["Street"].fillna("")
        df["Longitude"] = df["Longitude"].fillna(0.0)
        df["Latitude"] = df["Latitude"].fillna(0.0)

        for date in all_date_list:
            df[date] = pd.to_datetime(df[date], errors = 'coerce')
        
        return df

    def get_dates(self):

        all_date_list = ["CHRG Date","INSP Date","Last INSP Date","Out of Business Date","Case Dec. Date","APP Status Date","APP Start Date","APP End Date","Temp Op Letter Issued","Temp Op Letter Expiration","LIC Start Date","LIC Exp Date","LIC Issue Date","LIC Filing Date","RSS Date"]
        df = self.type_cast(self.df,all_date_list)
        
        # date_df = pd.DataFrame(columns=["Name","Start Date","End Date","Longitude","Latitude"])
        res_list = []

        totlen = len(self.df["LBID"].unique())
        ticker = 0

        lbid_group = df.groupby("LBID")
        for lbid , out_group in lbid_group:

            name1 = out_group["Business Name"].max()
            name2 = out_group["Business Name 2"].max()
            name3 = out_group["Business Name 3"].max()
            name = max(name1,name2,name3)
            naics_code = out_group["NAICS"].max()
            naics_title = out_group["NAICS Title"].max()

            if out_group["LLID"].nunique() == 1:

                address = f'{out_group["Building Number"].max()} {out_group["Street"].max()}'
                longitude = out_group["Longitude"].min()
                latitude = out_group["Latitude"].max()
                llid = out_group["LLID"].max()
                bbl = out_group["BBL"].max()
                phone_num = out_group["Contact Phone"].max()

                mindate = self.get_lim_date_from_cols(out_group,all_date_list,False)
                maxdate = self.get_max_end(out_group)

                if mindate == maxdate:
                    maxdate += pd.Timedelta(days=365)

                if latitude != 0.0 and longitude != 0.0:
                    new_row = {'Name': name, 'Name 1': name1, 'Name 2': name2, 'Name 3': name3, 'LBID': lbid, 'LLID': llid, "BBL": bbl, "Contact Phone": phone_num, "Address": address, 'Start Date': mindate, 'End Date': maxdate, 'Longitude': longitude, 'Latitude': latitude,'NAICS Title': naics_title, 'NAICS': naics_code}
                    res_list.append(new_row)
                    # date_df = date_df.append(new_row, ignore_index = True)

            else:

                llid_ordered = list()
                date_list = ["CHRG Date","INSP Date","Last INSP Date","Out of Business Date","Case Dec. Date","APP Status Date","APP Start Date","APP End Date","Temp Op Letter Issued","Temp Op Letter Expiration"]
                for llid in out_group["LLID"].unique():
                    in_group = out_group[out_group["LLID"] == llid]
                    mindate = self.get_lim_date_from_cols(in_group,date_list,False)
                    llid_ordered.append((llid,mindate))
                    
                llid_ordered.sort(key=lambda y: y[1],reverse=True)
                llid_ordered = [llid for llid,_ in llid_ordered]

                prevmin = pd.to_datetime("today")

                for llid_val in range(len(llid_ordered)):
                    in_group = out_group[out_group["LLID"] == llid]

                    address = f'{in_group["Building Number"].max()} {in_group["Street"].max()}'
                    longitude = in_group["Longitude"].min()
                    latitude = in_group["Latitude"].max()
                    bbl = in_group["BBL"].max()
                    phone_num = in_group["Contact Phone"].max()
                    llid = llid_ordered[llid_val]

                    if llid_val == len(llid_ordered)-1:
                        mindate = self.get_lim_date_from_cols(out_group,all_date_list,False)
                    else:
                        mindate = self.get_lim_date_from_cols(in_group,date_list,False)

                    if llid_val == 0:
                        maxdate = self.get_max_end(out_group)
                    else:
                        maxdate = prevmin

                    if mindate == maxdate:
                        maxdate += pd.Timedelta(days=365)

                    prevmin = mindate

                    if latitude != 0.0 and longitude != 0.0:
                        new_row = {'Name': name, 'Name 1': name1, 'Name 2': name2, 'Name 3': name3, 'LBID': lbid, 'LLID': llid, "BBL": bbl, "Contact Phone": phone_num, "Address": address, 'Start Date': mindate, 'End Date': maxdate, 'Longitude': longitude, 'Latitude': latitude, 'NAICS Title': naics_title, 'NAICS': naics_code}
                        # date_df = date_df.append(new_row, ignore_index = True)
                        res_list.append(new_row)
            ticker += 1
            print(f" {ticker} / {totlen}",end="\r")
        
        date_df = pd.DataFrame(res_list)
        cleaned_file_path = f"{DirectoryFields.S3_PATH}data/temp/timeline.csv"
        date_df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-timeline-nosubway.p', Body=pickle.dumps(date_df))
        return date_df
    
    def add_features(self, timeline_df):
        self.count = 0
        lendf = len(timeline_df)

        brands = {"mcdonalds","starbucks","lululemon","walmart","apple","best buy","kohls","nordstrom","bed bath & beyond"}
        subway_df = self.generate_subway()

        timeline_df["Subway"] = 0.0
        timeline_df["Brand"] = 0

        def add(row):
            if self.count % 10000 == 0:
                print(f"{self.count} / {lendf}")
            self.count += 1
            
            def is_brand(name):
                for brand in brands:
                    r1 = fuzz.token_sort_ratio(name, brand) > 50
                    r2 = fuzz.WRatio(name, brand) > 80
                    if r1 and r2:  
                        return 1
                return 0

            row["Subway"] = min(subway_df['geometry'].distance(row["geometry"]))
            row["Brand"] = is_brand(row["Name_clean"])
            return row

        gtimeline_df = geopandas.GeoDataFrame(timeline_df, geometry=geopandas.points_from_xy(timeline_df.Longitude, timeline_df.Latitude))
        gtimeline_df["Name_clean"] = gtimeline_df["Name"].apply(lambda x: x.lower())
        gtimeline_df = gtimeline_df.apply(lambda row: add(row),axis=1)
        del gtimeline_df["Name_clean"]

        # brand_df = gtimeline_df.copy()
        # brand_df = brand_df[brand_df["Brand"]==1]

        # self.count = 0
        # def add_brand_proximity(row):
        #     if self.count % 10000 == 0:
        #         print(f"{self.count} / {lendf}")
        #     self.count += 1
        #     prox = brand_df['geometry'].distance(row["geometry"])
        #     return len(prox[prox<=.002])

        # gtimeline_df["Brand Proximity"] = gtimeline_df.apply(lambda row: add_brand_proximity(row),axis=1)

        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-timeline.p', Body=pickle.dumps(gtimeline_df))

    # def add_crime(self, df):

    #     path = f"nypd/source/NYPD_Complaint_Data_Historic.csv"
    #     nypd_df = pd.read_csv(f"{DirectoryFields.S3_PATH}{path}", sep=",",low_memory=False)
    #     nypd_df['year'] = pd.to_datetime(nypd_df['CMPLNT_FR_DT'], errors = 'coerce').dt.year
    #     nypd_df = nypd_df[~nypd_df['year'].isna()]
    #     nypd_df['year'] = nypd_df['year'].astype(int)
    #     nypd_df=nypd_df[nypd_df["year"]>2010].sample(frac=.01)
    #     self.gnypd = geopandas.GeoDataFrame(nypd_df, geometry=geopandas.points_from_xy(nypd_df.Longitude, nypd_df.Latitude))
    #     print(len(nypd_df))
    #     def crime(row):
    #         distances = self.gnypd["geometry"].distance(row["geometry"])
    #         if self.merged_count % 100 == 0:
    #             print(f"{self.merged_count} / {self.merged_len}")
    #         self.merged_count += 1
    #         # print(len(distances[distances<.001])) ``
    #         row["Crime"] = len(distances[distances<.01])
    #         return row

    #     df['Crime'] = 0
    #     self.merged_count = 0
    #     self.merged_len = len(df)
    #     df = df.apply(lambda row: crime(row),axis=1)
    #     self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key='data/temp/df-timeline.p', Body=pickle.dumps(df))
    #     return df

if __name__ == "__main__":
    industry_assign = CreateTimeline()
    df = industry_assign.get_dates()
    industry_assign.add_features(df)
    # df = pickle.loads(industry_assign.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object('data/temp/df-timeline-nocrime.p').get()['Body'].read()) #DELETE