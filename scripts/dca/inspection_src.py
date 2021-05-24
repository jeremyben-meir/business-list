#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    inspection_10_17file_path = LOCAL_LOCUS_PATH + "data/dca/DCA_Inspections_10-17.csv"
    inspection_14_21file_path = LOCAL_LOCUS_PATH + "data/dca/DCA_Inspections_14-21.csv"

    df_10_17 = pd.read_csv(inspection_10_17file_path)
    df_14_21 = pd.read_csv(inspection_14_21file_path)

    col_10_17 = ['REC_ID','CERT_NBR','BIZ_NAME','INSP_DT','INSP_RSLT','INDUSTRY','BORO','BLDG_NBR','STREET','STREET2','UNIT_TYP','UNIT','DESCR','CITY','STATE','ZIP','X_COORD','Y_COORD']
    col_14_21 = ['Record ID','Certificate Number','Business Name','Inspection Date','Inspection Result','Industry','Borough','Building Number','Street','Street 2','Unit Type','Unit','Description','City','State','Zip','Longitude','Latitude']
    df_10_17.columns = col_14_21

    df = pd.concat([df_10_17,df_14_21], ignore_index=True)

    business_indicators_noninformative = ["Business Padlocked", "Unable to Locate", "Unable to Complete Inspection", "Unable to Seize Vehicle"]

    df["Inspection Result"] = df["Inspection Result"].astype(str)
    df['Borough'] = df['Borough'].astype(str).apply(lambda x: x.upper())
    df["Inspection Date"] = df["Inspection Date"].astype('datetime64[D]')
    df['Inspection Date'] = pd.to_datetime(df['Inspection Date']).dt.date
    df['Business Name'] = df['Business Name'].astype(str)
    df['Building Number'] = df['Building Number'].astype(str)
    df['Zip'] = df['Zip'].astype(str)
    df['Street'] = df['Street'].astype(str)
    df = df[~df["Inspection Result"].isin(business_indicators_noninformative)]# apply(lambda x: any([x == k for k in business_indicators_noninformative]))]
    df = df[df["Borough"] != "Outside NYC"]

    df = df.drop_duplicates(subset=['Borough', 'Business Name', 'Building Number', 'Street'], keep='first')

    df["City"] = df["City"].astype(str)
    df["City"] = df["City"].fillna("")
    df = df[(df['City'] == "") | (df['City'].isin(nyc_city))]

    df["Zip"] = df["Zip"].astype(str)
    df["Zip"] = df["Zip"].fillna("")
    df["Zip"] = df["Zip"].apply(lambda x: str(int(float(x))) if x != "" and not math.isnan(float(x)) else "")
    df = df[(df['Zip'] == "") | (df['Zip'].isin(manhattan_zips + non_manhattan_zips))]
    
    del df["Certificate Number"]
    del df["Borough"]
    del df["Longitude"]
    del df["Latitude"]
    del df["Inspection Result"]
    df = df.rename(columns={"Inspection Date": "INSP Date"})

    df['BBL'] = ""
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row), axis=1)

    return df

def begin_process():
    df = process_0()
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "wb" ))
        
if __name__ == '__main__':
    begin_process()