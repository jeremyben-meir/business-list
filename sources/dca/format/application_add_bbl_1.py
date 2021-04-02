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
    application_98_21file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/License_Applications_98_21.csv"
    application_00_12file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/License_Applications_00_12.csv"
    
    df_application_98_21 = pd.read_csv(application_98_21file_path)
    df_application_00_12 = pd.read_csv(application_00_12file_path)

    df_application = pd.concat([df_application_98_21,df_application_00_12], ignore_index=True)

    df_application['Start Date'] = df_application['Start Date'].astype('datetime64[D]')
    df_application['Business Name'] = df_application['Business Name'].astype(str)
    df_application['Contact Phone'] = df_application['Contact Phone'].astype(str).apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ",""))
    df_application['License Number'] = df_application['License Number'].astype(str)

    df_application['License Type'] = df_application['License Type'].astype(str)
    df_application['Application ID'] = df_application['Application ID'].astype(str)
    df_application['Application or Renewal'] = df_application['Application or Renewal'].astype(str)
    df_application['Temp Op Letter Issued'] = df_application['Temp Op Letter Issued'].astype('datetime64[D]')
    df_application['Temp Op Letter Expiration'] = df_application['Temp Op Letter Expiration'].astype('datetime64[D]')
    df_application['Application Category'] = df_application['Application Category'].astype(str)
    df_application['Unit Type'] = df_application['Unit Type'].astype(str)
    df_application['Zip'] = df_application['Zip'].astype(str)
    df_application['Building Number'] = df_application['Building Number'].astype(str)
    df_application['Street'] = df_application['Street'].astype(str)


    df_application = df_application.drop_duplicates(subset=['City', 'Street', 'Building Number', 'Start Date', 'End Date', 'Business Name', 'License Category'], keep='first')

    df_application['BBL'] = np.nan
    df_application['BBL Latest'] = np.nan

    df_application["City"] = df_application["City"].astype(str)
    df_application["City"] = df_application["City"].fillna("")
    df_application = df_application[(df_application['City'] == "") | (df_application['City'].isin(nyc_city))]
    
    df_application["Zip"] = df_application["Zip"].astype(str)
    df_application["Zip"] = df_application["Zip"].fillna("")
    df_application["Zip"] = df_application["Zip"].apply(lambda x: str(int(x)) if str(x).isdigit() else "")
    df_application = df_application[(df_application['Zip'] == "") | (df_application['Zip'].isin(manhattan_zips + non_manhattan_zips))]

    pickle.dump(df_application, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-1.p", "wb" ))
    return df_application

def process_1(df_1):
    totlen=len(df_1)
    df_1 = df_1.apply(lambda row: add_bbl(row, totlen=totlen), axis=1)

    pickle.dump(df_1, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "wb" ))
    return df_1

def begin_process():
    df_0 = process_0()
    # df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-1.p", "rb" ))
    
    df_1 = process_1(df_0)
    # df_1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "rb" ))

    df_1['Contact Phone'] = df_1['Contact Phone'].apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ",""))
    df_1['Date of First Application'] = df_1['Start Date']
    df_1['Date of Last Application'] = df_1['Start Date']
    df_1['Event Date'] = df_1['Start Date']

    del df_1["Application ID"]
    del df_1["Longitude"]
    del df_1["Latitude"]
    del df_1["Application or Renewal"]
    del df_1["Status"]
    del df_1["Application Category"]
    del df_1["End Date"]
    del df_1["Start Date"]
    df_1 = df_1.rename(columns={"License Number": "Record ID", "License Category": "Industry"})

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/application_updated_0.csv"
    df_1.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
if __name__ == '__main__':
    begin_process()