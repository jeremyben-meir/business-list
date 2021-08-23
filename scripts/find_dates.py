import pickle
from common import DirectoryFields
import pandas as pd

def extract_dates(row):
    pass

def get_dates():
    df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-merged.p", "rb" ))
    date_list = ["Out of Business Date","INSP Date","INSP Result","Last INSP Date","Case Dec. Date","LIC Issue Date","LIC Filing Date","LIC Expiration Date","LIC Status","LIC Start Date","LIC Exp Date","RSS","RSS Date","Application or Renewal","APP Status","APP Status Date","APP Start Date","APP End Date","Temp Op Letter Issued","Temp Op Letter Expiration"]
    for date in date_list:
        df[date] = pd.to_datetime(df[date], errors = 'coerce')
    date_df = pd.DataFrame(columns=["Name","Start Date","End Date","Longitude","Latitude"])
    group = df.groupby("LLID")
    for llid, curgrp in group:
        # print(curgrp[date_list])

        mindate = [curgrp[date].min() for date in date_list]
        maxdate = [curgrp[date].max() for date in date_list]

        mindate = [date for date in mindate if not pd.isnull(date)]
        maxdate = [date for date in maxdate if not pd.isnull(date)]

        mindate = min(mindate) if len(mindate) > 0 else pd.NaT
        maxdate = max(maxdate) if len(maxdate) > 0 else pd.NaT
        
        # print(mindate)
        # print(maxdate)
        
        if pd.isnull(mindate):
            print(llid)

        new_row = {'Name': "", 'Start Date': mindate, 'End Date': maxdate, 'Longitude': 93, 'Latitude': 93}
        date_df = date_df.append(new_row, ignore_index = True)

if __name__ == "__main__":
    get_dates()