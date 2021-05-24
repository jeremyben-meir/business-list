#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    inspection_10_17file_path = LOCAL_LOCUS_PATH + "data/dca/DCA_Inspections_10-17.csv"
    inspection_14_21file_path = LOCAL_LOCUS_PATH + "data/dca/DCA_Inspections_14-21.csv"

    df_inspection_10_17 = pd.read_csv(inspection_10_17file_path)
    df_inspection_14_21 = pd.read_csv(inspection_14_21file_path)

    col_10_17 = ['REC_ID','CERT_NBR','BIZ_NAME','INSP_DT','INSP_RSLT','INDUSTRY','BORO','BLDG_NBR','STREET','STREET2','UNIT_TYP','UNIT','DESCR','CITY','STATE','ZIP','X_COORD','Y_COORD']
    col_14_21 = ['Record ID','Certificate Number','Business Name','Inspection Date','Inspection Result','Industry','Borough','Building Number','Street','Street 2','Unit Type','Unit','Description','City','State','Zip','Longitude','Latitude']
    df_inspection_10_17.columns = col_14_21

    df_inspection = pd.concat([df_inspection_10_17,df_inspection_14_21], ignore_index=True)

    business_indicators_noninformative = ["Business Padlocked", "Unable to Locate", "Unable to Complete Inspection", "Unable to Seize Vehicle"]

    df_inspection["Inspection Result"] = df_inspection["Inspection Result"].astype(str)
    df_inspection['Borough'] = df_inspection['Borough'].astype(str).apply(lambda x: x.upper())
    df_inspection["Inspection Date"] = df_inspection["Inspection Date"].astype('datetime64[D]')
    df_inspection['Inspection Date'] = pd.to_datetime(df_inspection['Inspection Date']).dt.date
    df_inspection['Business Name'] = df_inspection['Business Name'].astype(str)
    df_inspection['Building Number'] = df_inspection['Building Number'].astype(str)
    df_inspection['Zip'] = df_inspection['Zip'].astype(str)
    df_inspection['Street'] = df_inspection['Street'].astype(str)
    df_inspection = df_inspection[~df_inspection["Inspection Result"].isin(business_indicators_noninformative)]# apply(lambda x: any([x == k for k in business_indicators_noninformative]))]
    df_inspection = df_inspection[df_inspection["Borough"] != "Outside NYC"]

    df_inspection = df_inspection.drop_duplicates(subset=['Borough', 'Business Name', 'Building Number', 'Street'], keep='first')

    df_inspection['BBL'] = np.nan

    df_inspection["City"] = df_inspection["City"].astype(str)
    df_inspection["City"] = df_inspection["City"].fillna("")
    df_inspection = df_inspection[(df_inspection['City'] == "") | (df_inspection['City'].isin(nyc_city))]

    df_inspection["Zip"] = df_inspection["Zip"].astype(str)
    df_inspection["Zip"] = df_inspection["Zip"].fillna("")
    df_inspection["Zip"] = df_inspection["Zip"].apply(lambda x: str(int(float(x))) if x != "" and not math.isnan(float(x)) else "")
    df_inspection = df_inspection[(df_inspection['Zip'] == "") | (df_inspection['Zip'].isin(manhattan_zips + non_manhattan_zips))]
    
    del df_inspection["Certificate Number"]
    del df_inspection["Borough"]
    del df_inspection["Longitude"]
    del df_inspection["Latitude"]
    del df_inspection["Inspection Result"]
    df_inspection = df_inspection.rename(columns={"Inspection Date": "INSP Date"})

    return df_inspection

def begin_process():
    df = process_0()
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-insp.p", "wb" ))
        
if __name__ == '__main__':
    begin_process()