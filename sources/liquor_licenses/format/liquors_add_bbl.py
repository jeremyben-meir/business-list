from global_vars import *

def edit_serial(my_row):
    if my_row["Serial"] == "" or pd.isna(my_row["Serial"]) or pd.isnull(my_row["Serial"]):
        my_row["Serial"] = my_row["Serial 2"]
        my_row["Serial 2"] = ""
    
    return my_row

def add_bbl(progress, my_row):
    progress.display()
    
    # print(my_row['Filing Date'])

    year = '9' if my_row['Filing Date'] != "" and my_row['Filing Date'] != "nan" and my_row['Filing Date'] != np.datetime64('NaT') and not pd.isna(my_row['Filing Date'] != "") and pd.isnull(my_row['Filing Date'] != "") else str(my_row['Filing Date'].year)[-1]
    try:
        last = ('3' if int(year) <= 3 else '5' if int(year) <= 5 else year)
    except:
        last = '9'

    if my_row["Address Tab"][-5:].isdigit():
        entry = my_row["Address Tab"][:my_row["Address Tab"].index('\n')] + " " + my_row["Address Tab"][-5:]
    else:
        entry = my_row["Address Tab"][:my_row["Address Tab"].index('\n')] + " " + my_row["Address Tab"][-10:-5]
        print(entry)

        
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

def begin_process(package):
    if package:
        df_liq_file = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-2.p", "rb"))

        df_liq_file['Filing Date'] = pd.to_datetime(df_liq_file['Filing Date'], errors='coerce')
        df_liq_file['BBL'] = np.nan
        df_liq_file['BBL Latest'] = np.nan

        progress = Progress_meter(limit=len(df_liq_file.index))
        df_liq_file = df_liq_file.apply(lambda row: add_bbl(progress, row), axis=1)
        
        pickle.dump(df_liq_file, open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-3.p", "wb"))
        df_liq_file.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/liquor_licenses_bbl.csv", index=False, quoting=csv.QUOTE_ALL)
    
    df_liq_file = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-3.p", "rb"))
    df_liq_file = df_liq_file.apply(lambda row: edit_serial(row), axis=1)

    del df_liq_file["Serial 2"]

    pickle.dump(df_liq_file, open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-4.p", "wb"))
    df_liq_file.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/liquor_licenses_bbl.csv", index=False, quoting=csv.QUOTE_ALL)



if __name__ == '__main__':
    begin_process(package=False)