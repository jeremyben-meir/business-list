#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    charge_file_path = LOCAL_LOCUS_PATH + "data/dca/Charges_10_21.csv"
    df = pd.read_csv(charge_file_path)

    df['Violation Date'] = df['Violation Date'].astype('datetime64[D]')
    df['Business Name'] = df['Business Name'].astype(str)
    df['Building Number'] = df['Building Number'].astype(str)
    df['Record ID'] = df['Record ID'].astype(str)
    df['Street'] = df['Street'].astype(str)
    df['Zip'] = df['Zip'].astype(str)

    df = df.drop_duplicates(subset=['City', 'Street', 'Building Number', 'Violation Date', 'Business Name', 'Industry'], keep='first')

    df["City"] = df["City"].astype(str)
    df["City"] = df["City"].fillna("")
    df = df[(df['City'] == "") | (df['City'].isin(nyc_city))]
    
    df["Zip"] = df["Zip"].astype(str)
    df["Zip"] = df["Zip"].fillna("")
    df["Zip"] = df["Zip"].apply(lambda x: str(int(float(x))) if x != "" and not math.isnan(float(x)) else "")
    df = df[(df['Zip'] == "") | (df['Zip'].isin(manhattan_zips + non_manhattan_zips))]

    del df["Certificate Number"]
    del df["Borough"]
    del df["Charge"]
    del df["Charge Count"]
    del df["Outcome"]
    del df["Counts Settled"]
    del df["Counts Guilty"]
    del df["Counts Not Guilty"]
    del df["Longitude"]
    del df["Latitude"]

    df = df.rename(columns={"Violation Date": "CHRG Date"})

    df['BBL'] = ""
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row), axis=1)

    return df

def begin_process():
    df = process_0()
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.p", "wb" ))

if __name__ == "__main__":
    begin_process()