#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

global counter
counter = 0

def add_bbl(row,totlen):
    # stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
    # stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]       
    global counter
    inject = row['Building Number'] + " " + row['Street'] + " " + row['Zip']
    try:
        response = requests.get("https://api.cityofnewyork.us/geoclient/v1/search.json?input="+ inject +"&app_id=d4aa601d&app_key=f75348e4baa7754836afb55dc9b6363d")
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        row["BBL"]=json_loaded['results'][0]['response']['bbl']
        # print("BBL " + str(json_loaded['results'][0]['response']['bbl']))
    except:
        # print(inject)
        row["BBL"]=""

    counter+=1
    # print(counter/totlben)
    if round(counter/totlen,4)>round((counter-1)/totlen,4):
        print(str(round(100*counter/totlen,2)) + "%")
    return row


def process_0():
    inspection_10_17file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/DCA_Inspections_10-17.csv"
    inspection_14_21file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/DCA_Inspections_14-21.csv"

    df_inspection_10_17 = pd.read_csv(inspection_10_17file_path)
    df_inspection_14_21 = pd.read_csv(inspection_14_21file_path)

    col_10_17 = ['REC_ID','CERT_NBR','BIZ_NAME','INSP_DT','INSP_RSLT','INDUSTRY','BORO','BLDG_NBR','STREET','STREET2','UNIT_TYP','UNIT','DESCR','CITY','STATE','ZIP','X_COORD','Y_COORD']
    col_14_21 = ['Record ID','Certificate Number','Business Name','Inspection Date','Inspection Result','Industry','Borough','Building Number','Street','Street 2','Unit Type','Unit','Description','City','State','Zip','Longitude','Latitude']
    df_inspection_10_17.columns = col_14_21
    # for x in range(0,len(col_10_17)):
    #     print({col_10_17[x]:col_14_21[x]})
    #     df_inspection_10_17.rename(columns={col_10_17[x]:col_14_21[x]}, errors="raise", axis='columns')
    # print(df_inspection_10_17)

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
    df_inspection['BBL Latest'] = np.nan

    df_inspection["City"] = df_inspection["City"].astype(str)
    df_inspection["City"] = df_inspection["City"].fillna("")
    df_inspection = df_inspection[(df_inspection['City'] == "") | (df_inspection['City'].isin(nyc_city))]

    df_inspection["Zip"] = df_inspection["Zip"].astype(str)
    df_inspection["Zip"] = df_inspection["Zip"].fillna("")
    df_inspection["Zip"] = df_inspection["Zip"].apply(lambda x: str(int(float(x))) if x != "" and not math.isnan(float(x)) else "")
    df_inspection = df_inspection[(df_inspection['Zip'] == "") | (df_inspection['Zip'].isin(manhattan_zips + non_manhattan_zips))]

    pickle.dump(df_inspection, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-1.p", "wb" ))
    return df_inspection

def process_1(df_1):
    totlen=len(df_1)
    df_1 = df_1.apply(lambda row: add_bbl(row, totlen=totlen), axis=1)

    pickle.dump(df_1, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "wb" ))
    return df_1

def begin_process():
    df_0 = process_0()
    # df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-1.p", "rb" ))
    
    df_1 = process_1(df_0)
    # df_1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "rb" ))
    df_1['Event Date'] = df_1['Inspection Date']

    del df_1["Certificate Number"]
    del df_1["Borough"]
    del df_1["Longitude"]
    del df_1["Latitude"]
    del df_1["Inspection Date"]
    del df_1["Inspection Result"]

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/inspections_updated_0.csv"
    df_1.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
if __name__ == '__main__':
    begin_process()