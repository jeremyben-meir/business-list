#######IMPORTS#######

from classes.common import DirectoryFields
from classes.source_file import SourceFile, pd, pickle, csv

#######FUNCTION DEFINITIONS#########

def instantiate_file(source):
    dh_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doh/doh_10-20.txt"
    textfile = open(dh_file_path, "r")
    colnames = textfile.readline().strip("\n").split("\t")
    lines = textfile.readlines()
    listlist = [x.strip("\n").split("\t") for x in lines]
    df = pd.DataFrame(listlist, columns=colnames)

    df = df.rename(columns={"CAMIS": "Record ID", 'DBA':'Business Name', 'BORO':'City', 'ZIPCODE':'Zip','PHONE':'Contact Phone', 'CUISINECODE':'Industry','INSPDATE':'INSP Date','CASE_DECISION_DATE':'Case Dec. Date','CURRENTGRADE':'Current Grade','GRADEDATE':'Current Grade Date'})
    
    df['City'] = df['City'].replace(['0','1','2','3',"4","5"],['','NEW YORK', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND'])
    df['Building Number'] = df['ADDRESS'].apply(lambda x: x.split(" ")[0])
    df['Street'] = df['ADDRESS'].apply(lambda x: " ".join(x.split(" ")[1:]))
    df['Industry'] = df['Industry'].apply(lambda x: "Restaurant-"+x)
    df['Industry'] = df['Industry'].apply(lambda x: x.strip('        '))
    df['State'] = 'NY'
    df['INSP Date'] = df['INSP Date'].astype('datetime64[D]')
    df['Case Dec. Date'] = df['Case Dec. Date'].replace(['N/A','NULL','nan'],'')
    df['Case Dec. Date'] = df['Case Dec. Date'].astype('datetime64[D]')
    df['Current Grade'] = df['Current Grade'].astype(str)
    df['Current Grade Date'] = df['Current Grade Date'].replace(['N/A','NULL','nan'],'')
    df['Current Grade Date'] = df['Current Grade Date'].astype('datetime64[D]')

    del df['ADDRESS']
    del df['ACTION']
    del df['PROGRAM']
    del df['INSPTYPE']
    del df['VIOLCODE']
    del df['DISMISSED']
    del df['SCORE']
    del df['VIOLSCORE']
    del df['MOD_TOTALSCORE']

    df = source.type_cast(df)
    df = source.clean_zip_city(df)
    df = df.drop_duplicates()
    
    return df

def begin_process(segment):
    source = SourceFile()

    if 0 in segment:
        df = instantiate_file(source)
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doh/temp/df-doh.p", "wb" ))

    if 1 in segment:
        df = pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doh/temp/df-doh.p", "rb" ))
        df = source.add_bbl_async(df)
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doh/temp/df-doh-1.p", "wb" ))

    cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doh/temp/inspections.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        
if __name__ == '__main__':
    begin_process([1])