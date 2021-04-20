#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

global counter
counter = 0

# def add_bbl(row,correct_nyc_city,nyc_city):
#     global meter,prior,limit
#     meter,prior = progress_meter(meter=meter, limit=limit, prior=prior)

#     my_row = row
#     year = str(row['Violation Date'].year)[-1]
#     last = ('3' if int(year) <= 3 else '5' if int(year) <= 5 else year)

#     my_row['City'] = correct_nyc_city[nyc_city.index(row['City'])] if row['City'] != "" else ""

#     entry = (str(my_row['Building Number']) + " " + str(my_row['Street']) + " " + str(my_row['City']) if not my_row['Zip'].isdigit() else str(my_row['Building Number']) + " " + str(my_row['Street']) + " " + str(my_row['Zip']))
#     try:
#         response = requests.get("http://localhost:808"+last+"/geoclient/v2/search.json?input="+ entry)
#         decoded = response.content.decode("utf-8")
#         json_loaded = json.loads(decoded)
#         my_row["BBL"]=json_loaded['results'][0]['response']['bbl']
#     except:
#         my_row["BBL"]=""

#     try:
#         response = requests.get("http://localhost:8089/geoclient/v2/search.json?input="+ entry)
#         decoded = response.content.decode("utf-8")
#         json_loaded = json.loads(decoded)
#         my_row["BBL Latest"]=json_loaded['results'][0]['response']['bbl']
#     except:
#         my_row["BBL Latest"]=""


#     return my_row

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
    charge_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/Charges_10_21.csv"
    df_charge = pd.read_csv(charge_file_path)

    df_charge['Violation Date'] = df_charge['Violation Date'].astype('datetime64[D]')
    df_charge['Business Name'] = df_charge['Business Name'].astype(str)
    df_charge['Building Number'] = df_charge['Building Number'].astype(str)
    df_charge['Record ID'] = df_charge['Record ID'].astype(str)
    df_charge['Street'] = df_charge['Street'].astype(str)
    df_charge['Zip'] = df_charge['Zip'].astype(str)

    df_charge = df_charge.drop_duplicates(subset=['City', 'Street', 'Building Number', 'Violation Date', 'Business Name', 'Industry'], keep='first')

    df_charge['BBL'] = np.nan

    df_charge["City"] = df_charge["City"].astype(str)
    df_charge["City"] = df_charge["City"].fillna("")
    df_charge = df_charge[(df_charge['City'] == "") | (df_charge['City'].isin(nyc_city))]
    
    df_charge["Zip"] = df_charge["Zip"].astype(str)
    df_charge["Zip"] = df_charge["Zip"].fillna("")
    df_charge["Zip"] = df_charge["Zip"].apply(lambda x: str(int(float(x))) if x != "" and not math.isnan(float(x)) else "")
    df_charge = df_charge[(df_charge['Zip'] == "") | (df_charge['Zip'].isin(manhattan_zips + non_manhattan_zips))]

    del df_charge["Certificate Number"]
    del df_charge["Borough"]
    del df_charge["Charge"]
    del df_charge["Charge Count"]
    del df_charge["Outcome"]
    del df_charge["Counts Settled"]
    del df_charge["Counts Guilty"]
    del df_charge["Counts Not Guilty"]
    del df_charge["Longitude"]
    del df_charge["Latitude"]

    df_charge = df_charge.rename(columns={"Violation Date": "CHRG Date"})

    pickle.dump(df_charge, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-1.p", "wb" ))
    return df_charge

def process_1(df_0):
    totlen = len(df_0)
    df_0 = df_0.apply (lambda row: add_bbl(row,totlen), axis=1)

    pickle.dump(df_0, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "wb" ))
    return df_0

def begin_process():
    df_0 = process_0()
    # df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-1.p", "rb" ))

    df_1 = process_1(df_0)
    # df_1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "rb" ))

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/charge_updated_0.csv"
    df_1.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    begin_process()