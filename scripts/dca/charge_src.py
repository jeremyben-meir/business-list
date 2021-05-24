#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    charge_file_path = LOCAL_LOCUS_PATH + "data/dca/Charges_10_21.csv"
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

    return df_charge

def begin_process():
    df = process_0()
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.p", "wb" ))

if __name__ == "__main__":
    begin_process()