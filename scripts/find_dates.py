import pickle
from common import DirectoryFields
import pandas as pd
import csv

def extract_dates(row):
    pass

def get_dates():
    df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-merged.p", "rb" ))
    df["Business Name"] = df["Business Name"].fillna("")
    df["Business Name 2"] = df["Business Name 2"].fillna("")
    df["Building Number"] = df["Building Number"].fillna("")
    df["Street"] = df["Street"].fillna("")
    # df["Longitude"] = df["Longitude"].fillna(0)
    # df["Latitude"] = df["Latitude"].fillna(0)
    date_list = ["Out of Business Date","INSP Date","INSP Result","Last INSP Date","Case Dec. Date","LIC Issue Date","LIC Filing Date","LIC Expiration Date","LIC Status","LIC Start Date","LIC Exp Date","RSS","RSS Date","Application or Renewal","APP Status","APP Status Date","APP Start Date","APP End Date","Temp Op Letter Issued","Temp Op Letter Expiration"]
    for date in date_list:
        df[date] = pd.to_datetime(df[date], errors = 'coerce')
    date_df = pd.DataFrame(columns=["Name","Start Date","End Date","Longitude","Latitude"])
    group = df.groupby("LLID")
    for llid, curgrp in group:

        name = max(curgrp["Business Name"].max(),curgrp["Business Name 2"].max())
        address = f'{curgrp["Building Number"].max()} {curgrp["Street"].max()}'
        longitude = 0#curgrp["Longitude"].max()
        latitude = 0#curgrp["Latitude"].max()

        mindate = [curgrp[date].min() for date in date_list]
        maxdate = [curgrp[date].max() for date in date_list]

        mindate = [date for date in mindate if not pd.isnull(date)]
        maxdate = [date for date in maxdate if not pd.isnull(date)]

        if len(mindate) > 0:
            new_row = {'Name': name, 'Start Date': min(mindate), "Address": address, 'End Date': max(maxdate), 'Longitude': longitude, 'Latitude': latitude}
            date_df = date_df.append(new_row, ignore_index = True)
            if len(date_df) >= 500:
                cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/timeline.csv"
                date_df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
                break

if __name__ == "__main__":
    get_dates()