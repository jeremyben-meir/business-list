#######IMPORTS#######

from global_vars import LOCAL_LOCUS_PATH
from classes.SourceFile import SourceFile, pd, pickle, csv

#######FUNCTION DEFINITIONS#########

def instantiate_file(source):
    charge_file_path = LOCAL_LOCUS_PATH + "data/dca/Charges_10_21.csv"
    df = pd.read_csv(charge_file_path)

    df = df.rename(columns={"Violation Date": "CHRG Date"})

    df['Contact Phone'] = ""
    df['CHRG Date'] = df['CHRG Date'].astype('datetime64[D]')
    df['Street 2'] = df['Street 2'].astype(str)
    df['Unit Type'] = df['Unit Type'].astype(str)
    df['Unit'] = df['Unit'].astype(str)
    df['Description'] = df['Description'].astype(str)

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

    df = source.type_cast(df)
    df = source.clean_zip_city(df)
    df = df.drop_duplicates()

    return df

def begin_process(segment):
    source = SourceFile()

    if 0 in segment:
        df = instantiate_file(source)
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.p", "wb" ))

    if 1 in segment:
        df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge.p", "rb" ))
        df = source.add_bbl_async(df)
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-charge-1.p", "wb" ))

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/dca/temp/charges.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        
if __name__ == '__main__':
    begin_process([0])