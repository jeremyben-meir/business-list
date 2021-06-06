#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    charge_file_path = LOCAL_LOCUS_PATH + "data/dca/Charges_10_21.csv"
    df = pd.read_csv(charge_file_path)

    df['Record ID'] = df['Record ID'].astype(str)
    df['Business Name'] = df['Business Name'].astype(str)
    df['Violation Date'] = df['Violation Date'].astype('datetime64[D]')
    df['Industry'] = df['Industry'].astype(str)
    df['Building Number'] = df['Building Number'].astype(str)
    df['Street'] = df['Street'].astype(str)
    df['Street 2'] = df['Street 2'].astype(str)
    df['Unit Type'] = df['Unit Type'].astype(str)
    df['Unit'] = df['Unit'].astype(str)
    df['Description'] = df['Description'].astype(str)

    global_counter_init(len(df))
    df = clean_zip_city(df)

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
    df = df.drop_duplicates()

    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.p", "wb" ))

def process_1():
    df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.p", "rb" ))
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row), axis=1)
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge-1.p", "wb" ))

def process_2():
    df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge-1.p", "rb" ))
    df.to_csv(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.csv")

if __name__ == "__main__":
    process_2()