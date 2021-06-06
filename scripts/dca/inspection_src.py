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
    df = df[~df["Inspection Result"].isin(business_indicators_noninformative)]
    df["Inspection Date"] = df["Inspection Date"].astype('datetime64[D]')
    df['Industry'] = df['Industry'].astype(str)
    df['Business Name'] = df['Business Name'].astype(str)
    df['Building Number'] = df['Building Number'].astype(str)
    df['Street'] = df['Street'].astype(str)
    df['Street 2'] = df['Street 2'].astype(str)
    df['Unit Type'] = df['Unit Type'].astype(str)
    df['Unit'] = df['Unit'].astype(str)
    df['Description'] = df['Description'].astype(str)

    global_counter_init(len(df))
    df = clean_zip_city(df)

    del df["Certificate Number"]
    del df["Borough"]
    del df["Longitude"]
    del df["Latitude"]
       
    df = df.rename(columns={"Inspection Date": "INSP Date", 'Inspection Result':"INSP Result"})

    df['BBL'] = ""
    df = df.drop_duplicates()

    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "wb" ))

def process_1():
    df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "rb" ))
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row), axis=1)
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp-1.p", "wb" ))

def process_2():
    df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp-1.p", "rb" ))
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "wb" ))
        
if __name__ == '__main__':
    process_1()