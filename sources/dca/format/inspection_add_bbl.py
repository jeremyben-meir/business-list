#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def add_rows(row):
    my_row = row

    my_row["Date of First Close"] = my_row["Inspection Date"] if my_row["Inspection Result"] == "Closed" else np.nan
    my_row["Date of Last Close"] = my_row["Inspection Date"] if my_row["Inspection Result"] == "Closed" else np.nan
    my_row["Date of First OOB"] = my_row["Inspection Date"] if my_row["Inspection Result"] == "Out of Business" else np.nan
    my_row["Date of Last OOB"] = my_row["Inspection Date"] if my_row["Inspection Result"] == "Out of Business" else np.nan
    my_row["Date of First Inspection"] = my_row["Inspection Date"] if my_row["Inspection Result"] != "Closed" and my_row["Inspection Result"] != "Out of Business" else np.nan
    my_row["Date of Last Inspection"] = my_row["Inspection Date"] if my_row["Inspection Result"] != "Closed" and my_row["Inspection Result"] != "Out of Business" else np.nan

    return my_row

def add_bbl(row,correct_nyc_city,nyc_city):
    global meter,prior,limit
    meter,prior = progress_meter(meter=meter, limit=limit, prior=prior)

    my_row = row
    year = str(row['Inspection Date'].year)[-1]
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
    
    my_row = add_rows(my_row)

    return my_row

def process_0():
    inspection_17file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/DCA_Inspections_17.csv"
    inspection_19file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/DCA_Inspections_19.csv"

    df_inspection_17 = pd.read_csv(inspection_17file_path)
    df_inspection_19 = pd.read_csv(inspection_19file_path)
    df_inspection = pd.concat([df_inspection_17,df_inspection_19], ignore_index=True)

    business_indicators_noninformative = ["Business Padlocked", "Unable to Locate", "Unable to Complete Inspection", "Unable to Seize Vehicle"]

    df_inspection["Inspection Result"] = df_inspection["Inspection Result"].astype(str)
    df_inspection['Borough'] = df_inspection['Borough'].astype(str).apply(lambda x: x.upper())
    df_inspection["Inspection Date"] = df_inspection["Inspection Date"].astype('datetime64[D]')
    df_inspection['Inspection Date'] = pd.to_datetime(df_inspection['Inspection Date']).dt.date
    df_inspection['Business Name'] = df_inspection['Business Name'].astype(str)
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

def process_1():
    df_0 = process_0()
    df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-1.p", "rb" ))

    global meter,prior,limit
    meter = 0
    prior = 0
    limit = len(df_0.index)
    df_0 = df_0.apply (lambda row: add_bbl(row,correct_nyc_city,nyc_city), axis=1)

    pickle.dump(df_0, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "wb" ))
    return df_0

def begin_process():
    # df_final = process_1()
    df_final = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "rb" ))
    df_final['Event Date'] = df_final['Inspection Date']

    del df_final["Certificate Number"]
    del df_final["Borough"]
    del df_final["Longitude"]
    del df_final["Latitude"]
    del df_final["Inspection Date"]
    del df_final["Inspection Result"]

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/inspections_updated_0.csv"
    df_final.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
if __name__ == '__main__':
    begin_process()