#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def progress_meter(meter, limit, prior):
    length = 1000.0
    progress = int((float(meter)/float(limit)) * length)
    global timestart
    global timecount
    if meter == 0:

        print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        print('Time remaining: calculating...')
        print("Writing CSV" + ": [" + "-"*int(length) + "]")

        timecount = 0.0
        timestart = time.time()
    elif progress - prior > 0:
        print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        print("CPU Usage: " + str(psutil.cpu_percent()))
        print("Memory Usage: " + str(psutil.virtual_memory().percent))

        timecount+=1.0
        x= datetime.datetime.now()
        seconds_away = int(float(time.time() - timestart)*(float(length)/float(timecount)) - float(time.time() - timestart))
        y = x + datetime.timedelta(0,seconds_away)

        print('Time remaining: ' + str(seconds_away)+ " seconds (" + str(y.strftime("%I:%M:%S%p")) + ")")
        print("Writing CSV" + ": [" + "#"*int(timecount) + "-"*int(length-timecount) + "] "+ str(int(progress/10.0))+"%")

    return meter + 1  , progress

def add_bbl(row,correct_nyc_city,nyc_city):
    global meter,prior,limit
    meter,prior = progress_meter(meter=meter, limit=limit, prior=prior)

    my_row = row
    year = str(row['Violation Date'].year)[-1]
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
    charge_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/Charge.csv"
    df_charge = pd.read_csv(charge_file_path)

    df_charge['Violation Date'] = df_charge['Violation Date'].astype('datetime64[D]')
    df_charge['Business Name'] = df_charge['Business Name'].astype(str)
    df_charge['Record ID'] = df_charge['Record ID'].astype(str)

    df_charge = df_charge.drop_duplicates(subset=['City', 'Street', 'Building Number', 'Violation Date', 'Business Name', 'Industry'], keep='first')

    df_charge['BBL'] = np.nan
    df_charge['BBL Latest'] = np.nan

    df_charge["City"] = df_charge["City"].astype(str)
    df_charge["City"] = df_charge["City"].fillna("")
    df_charge = df_charge[(df_charge['City'] == "") | (df_charge['City'].isin(nyc_city))]
    
    df_charge["Zip"] = df_charge["Zip"].astype(str)
    df_charge["Zip"] = df_charge["Zip"].fillna("")
    df_charge["Zip"] = df_charge["Zip"].apply(lambda x: str(int(float(x))) if x != "" and not math.isnan(float(x)) else "")
    df_charge = df_charge[(df_charge['Zip'] == "") | (df_charge['Zip'].isin(manhattan_zips + non_manhattan_zips))]

    print(df_charge["Zip"])

    pickle.dump(df_charge, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-1.p", "wb" ))
    return df_charge

def process_1():
    df_0 = process_0()
    df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-1.p", "rb" ))

    global meter,prior,limit
    meter = 0
    prior = 0
    limit = len(df_0.index)
    df_0 = df_0.apply (lambda row: add_bbl(row,correct_nyc_city,nyc_city), axis=1)

    pickle.dump(df_0, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "wb" ))
    return df_0

def begin_process():
    df_final = process_1()
    df_final = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "rb" ))

    df_final['Date of First Charge'] = df_final['Violation Date']
    df_final['Date of Last Charge'] = df_final['Violation Date']

    del df_final["Certificate Number"]
    del df_final["Violation Date"]
    del df_final["Borough"]
    del df_final["Charge"]
    del df_final["Charge Count"]
    del df_final["Outcome"]
    del df_final["Counts Settled"]
    del df_final["Counts Guilty"]
    del df_final["Counts Not Guilty"]
    del df_final["Longitude"]
    del df_final["Latitude"]

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/charge_updated_0.csv"
    df_final.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    begin_process()