#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def add_bbl(row,correct_nyc_city,nyc_city):
    global meter,prior,limit
    meter,prior = progress_meter(meter=meter, limit=limit, prior=prior)

    my_row = row
    year = str(row['Start Date'].year)[-1]
    last = ('3' if int(year) <= 3 else '5' if int(year) <= 5 else year)

    my_row['City'] = correct_nyc_city[nyc_city.index(row['City'])] if row['City'] != "" else ""

    entry = (str(my_row['Building Number']) + " " + str(my_row['Street']) + " " + str(my_row['City']) if not my_row['Zip'].isdigit() else str(my_row['Building Number']) + " " + str(my_row['Street']) + " " + str(my_row['Zip']))
    try:
        response = requests.get("http://localhost:808"+last+"/geoclient/v2/search.json?input="+ entry)
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        my_row["BBL"]=json_loaded['results'][0]['response']['bbl']
    except:
        my_row["BBL"]=""

    try:
        response = requests.get("http://localhost:8089/geoclient/v2/search.json?input="+ entry)
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        my_row["BBL Latest"]=json_loaded['results'][0]['response']['bbl']
    except:
        my_row["BBL Latest"]=""


    return my_row

def process_0():
    application_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/License_Applications.csv"
    df_application = pd.read_csv(application_file_path)

    df_application['Start Date'] = df_application['Start Date'].astype('datetime64[D]')
    df_application['Business Name'] = df_application['Business Name'].astype(str)
    df_application['Contact Phone'] = df_application['Contact Phone'].astype(str).apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ",""))
    df_application['License Number'] = df_application['License Number'].astype(str)

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

def process_1():
    df_0 = process_0()
    df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-1.p", "rb" ))

    global meter,prior,limit
    meter = 0
    prior = 0
    limit = len(df_0.index)
    df_0 = df_0.apply (lambda row: add_bbl(row,correct_nyc_city,nyc_city), axis=1)

    pickle.dump(df_0, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "wb" ))
    return df_0

def begin_process():
    # df_final = process_1()
    df_final = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "rb" ))

    df_final['Contact Phone'] = df_final['Contact Phone'].apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ",""))
    df_final['Date of First Application'] = df_final['Start Date']
    df_final['Date of Last Application'] = df_final['Start Date']
    df_final['Event Date'] = df_final['Start Date']

    del df_final["Application ID"]
    del df_final["Longitude"]
    del df_final["Latitude"]
    del df_final["Application or Renewal"]
    del df_final["Status"]
    del df_final["Application Category"]
    del df_final["End Date"]
    del df_final["Start Date"]
    df_final = df_final.rename(columns={"License Number": "Record ID", "License Category": "Industry"})

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/applications_updated_0.csv"
    df_final.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    begin_process()