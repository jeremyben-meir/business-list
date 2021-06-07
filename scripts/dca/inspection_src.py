#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def instantiate_file():
    # Get file paths
    inspection_10_17file_path = LOCAL_LOCUS_PATH + "data/dca/DCA_Inspections_10-17.csv"
    inspection_14_21file_path = LOCAL_LOCUS_PATH + "data/dca/DCA_Inspections_14-21.csv"

    # Get df from file paths
    df_10_17 = pd.read_csv(inspection_10_17file_path)
    df_14_21 = pd.read_csv(inspection_14_21file_path)

    # Adjust column headers to match
    col_10_17 = ['REC_ID','CERT_NBR','BIZ_NAME','INSP_DT','INSP_RSLT','INDUSTRY','BORO','BLDG_NBR','STREET','STREET2','UNIT_TYP','UNIT','DESCR','CITY','STATE','ZIP','X_COORD','Y_COORD']
    col_14_21 = ['Record ID','Certificate Number','Business Name','Inspection Date','Inspection Result','Industry','Borough','Building Number','Street','Street 2','Unit Type','Unit','Description','City','State','Zip','Longitude','Latitude']
    df_10_17.columns = col_14_21

    # Concatenate the two files
    df = pd.concat([df_10_17,df_14_21], ignore_index=True)

    # Rename appropriate rows
    df = df.rename(columns={"Inspection Date": "INSP Date", 'Inspection Result':"INSP Result"})

    business_indicators_noninformative = ["Business Padlocked", "Unable to Locate", "Unable to Complete Inspection", "Unable to Seize Vehicle"]

    df['Contact Phone'] = ""
    df["INSP Result"] = df["INSP Result"].astype(str)
    df = df[~df["INSP Result"].isin(business_indicators_noninformative)]
    df["INSP Date"] = df["INSP Date"].astype('datetime64[D]')
    df['Street 2'] = df['Street 2'].astype(str)
    df['Unit Type'] = df['Unit Type'].astype(str)
    df['Unit'] = df['Unit'].astype(str)
    df['Description'] = df['Description'].astype(str)

    del df["Certificate Number"]
    del df["Borough"]
    del df["Longitude"]
    del df["Latitude"]
       
    df = type_cast(df)
    df = clean_zip_city(df)

    return df

def begin_process(segment):
    if 0 in segment:
        df = instantiate_file()
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "wb" ))

    if 1 in segment:
        df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "rb" ))
        global_counter_init(len(df))
        df = df.apply(lambda row: add_bbl(row), axis=1)
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp-1.p", "wb" ))

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/dca/temp/inspections.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        
if __name__ == '__main__':
    begin_process([0])