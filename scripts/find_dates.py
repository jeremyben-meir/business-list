from datetime import date
import pickle
from common import DirectoryFields
import pandas as pd
import csv

def get_lim_date_from_cols(curgrp,col_list,is_maximum):
    lim_date = [(curgrp[date].max() if is_maximum else curgrp[date].min()) for date in col_list]
    lim_list = [date for date in lim_date if not pd.isnull(date)]
    if len(lim_list) == 0:
        return pd.NaT
    return max(lim_list) if is_maximum else min(lim_list)

def get_max_end(curgrp):
    insp_res = (curgrp["INSP Result"].max() == "Out of Business")
    app_res = (curgrp["APP Status"].max() == "OOB")
    maxdate_list = list()
    date_list = ["CHRG Date","INSP Date","Last INSP Date","Case Dec. Date","Temp Op Letter Expiration","APP End Date"]
    maxdate_list.append(get_lim_date_from_cols(curgrp,date_list,True) + pd.Timedelta(days=550))
    date_list = ["LIC Exp Date","RSS Date"]
    maxdate_list.append(get_lim_date_from_cols(curgrp,date_list,True))
    date_list = ["Out of Business Date"]
    if insp_res:
        date_list.append("INSP Date")
    if app_res:
        date_list.append("APP Status")
    maxdate_list.append(get_lim_date_from_cols(curgrp,date_list,False))
    return min(maxdate_list) if min(maxdate_list) < pd.to_datetime("today") else pd.to_datetime("today")

def type_cast(df, all_date_list):
    df["Business Name"] = df["Business Name"].fillna("")
    df["Business Name 2"] = df["Business Name 2"].fillna("")
    df["Building Number"] = df["Building Number"].fillna("")
    df["INSP Result"] = df["INSP Result"].fillna("")
    df["APP Status"] = df["APP Status"].fillna("")
    df["Street"] = df["Street"].fillna("")
    df["Longitude"] = df["Longitude"].fillna(0.0)
    df["Latitude"] = df["Latitude"].fillna(0.0)

    for date in all_date_list:
        df[date] = pd.to_datetime(df[date], errors = 'coerce')
    
    return df

def get_dates():
    df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-assigned.p", "rb" ))


    all_date_list = ["CHRG Date","INSP Date","Last INSP Date","Out of Business Date","Case Dec. Date","APP Status Date","APP Start Date","APP End Date","Temp Op Letter Issued","Temp Op Letter Expiration","LIC Start Date","LIC Exp Date","LIC Issue Date","LIC Filing Date","RSS Date"]
    df = type_cast(df,all_date_list)
    
    date_df = pd.DataFrame(columns=["Name","Start Date","End Date","Longitude","Latitude"])
    
    lbid_group = df.groupby("LBID")
    for _ , out_group in lbid_group:

        name = max(out_group["Business Name"].max(),out_group["Business Name 2"].max())

        if out_group["LLID"].nunique() == 1:

            address = f'{out_group["Building Number"].max()} {out_group["Street"].max()}'
            longitude = out_group["Longitude"].min()
            latitude = out_group["Latitude"].max()
            llid = out_group["LLID"].max()

            mindate = get_lim_date_from_cols(out_group,all_date_list,False)
            maxdate = get_max_end(out_group)

            if latitude != 0.0 and longitude != 0.0:
                new_row = {'Name': name, 'LLID': llid, "Address": address, 'Start Date': mindate, 'End Date': maxdate, 'Longitude': longitude, 'Latitude': latitude}
                date_df = date_df.append(new_row, ignore_index = True)

        else:

            llid_ordered = list()
            date_list = ["CHRG Date","INSP Date","Last INSP Date","Out of Business Date","Case Dec. Date","APP Status Date","APP Start Date","APP End Date","Temp Op Letter Issued","Temp Op Letter Expiration"]
            for llid in out_group["LLID"].unique():
                in_group = out_group[out_group["LLID"] == llid]
                mindate = get_lim_date_from_cols(in_group,date_list,False)
                llid_ordered.append((llid,mindate))
                
            llid_ordered.sort(key=lambda y: y[1],reverse=True)
            llid_ordered = [llid for llid,_ in llid_ordered]

            prevmin = pd.to_datetime("today")

            for llid_val in range(len(llid_ordered)):
                in_group = out_group[out_group["LLID"] == llid]

                address = f'{in_group["Building Number"].max()} {in_group["Street"].max()}'
                longitude = in_group["Longitude"].min()
                latitude = in_group["Latitude"].max()
                llid = llid_ordered[llid_val]

                if llid_val == len(llid_ordered)-1:
                    mindate = get_lim_date_from_cols(out_group,all_date_list,False)
                else:
                    mindate = get_lim_date_from_cols(in_group,date_list,False)

                if llid_val == 0:
                    maxdate = get_max_end(out_group)
                else:
                    maxdate = prevmin

                prevmin = mindate

                if latitude != 0.0 and longitude != 0.0:
                    new_row = {'Name': name, 'LLID': llid, "Address": address, 'Start Date': mindate, 'End Date': maxdate, 'Longitude': longitude, 'Latitude': latitude}
                    date_df = date_df.append(new_row, ignore_index = True)
        
    cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/timeline.csv"
    date_df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

if __name__ == "__main__":
    get_dates()