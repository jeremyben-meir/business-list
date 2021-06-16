#######IMPORTS#######

from classes.common import DirectoryFields
from classes.source_file import SourceFile, pd, pickle, csv

#######FUNCTION DEFINITIONS#########

def clean_addr(row):
    stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
    stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]
    endwords = ["AVE","AVENUE","ST","STREET","ROAD","RD","PLACE","PL","BOULEVARD","BLVD"]
    
    # address = row['Address'][0]
    # zipcode = row['Address'][1]

    if not any(x.isdigit() for x in row['Address'][0]):
        del row['Address'][0]
    if "United States" in row['Address'][-1]:
        del row['Address'][-1]

    if len(row['Address']) > 1:
        # print(row['Address'][0])
        row['Address'][0] = row['Address'][0].replace(",","")
        row['Address'][0] = row['Address'][0].replace(".","")
        row['Address'][0] = row['Address'][0].replace("\"","")
        row['Address'][0] = row['Address'][0].replace("-","")
        row['Address'][0] = row['Address'][0].strip(" ")
        
      
        for word in stopwords1:
            if word in row['Address'][0].upper():
                row['Address'][0] = row['Address'][0][:row['Address'][0].upper().index(word)]

        if any(word in row['Address'][0].upper() for word in stopwords):
            mylist = row['Address'][0].split(" ")
            for x in range(0, len(mylist)):
                if mylist[x].upper() in stopwords:
                    mylist = mylist[:x]
                    row['Address'][0] = " ".join(mylist)
                    break
            
        mylist = row['Address'][0].split(" ")
        for x in range(0, len(mylist)):
            if mylist[x].upper() in endwords:
                mylist = mylist[:x+1]
                row['Address'][0] = " ".join(mylist)
                break

        try:
            if row['Address'][0].split(" ")[1][:-1].isnumeric() or row['Address'][0].split(" ")[1].isnumeric():
                mylist = row['Address'][0].split(" ")
                mylist[1] = mylist[0] + "-" + mylist[1]
                del mylist[0]
                row['Address'][0] = " ".join(mylist)
        except:
            pass   

        if not " " in row['Address'][0]:
            row['Building Number']=""
            row['Street']=""
        else:
            row['Building Number'] = row['Address'][0].split(" ")[0]
            row['Street'] = " ".join(row['Address'][0].split(" ")[1:])

        if not (row['Building Number'].replace("-","").isnumeric()):
            row['Building Number']=""
            row['Street']=""

        row['Zip'] = row['Address'][1].split(" ")[-1]
        
        if "1/2" in row['Street']:
            row['Street'] = row['Street'][row['Street'].index("1/2")+3:]
      
        if not row['Zip'].isnumeric():
            if "-" in row["Zip"]:
                row["Zip"] = row["Zip"].split("-")[0]
            if row['Address'][-1][-1] == " ":
                row['Address'][-1] = row['Address'][-1][:-1]
            row['Zip'] = row['Address'][-1].split(" ")[-1][:5]
      
    row['Building Number'] = row['Building Number'].strip(" ")
    row['Street'] = row['Street'].strip(" ")
    row['Zip'] = row['Zip'].strip(" ")

    return row


def instantiate_file(source):
    # Get file paths
    aes_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/aes_10-20.csv"
    barber_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/barber_92-20.csv"
    
    # Get dfs from file paths
    df_aes = pd.read_csv(aes_file_path)
    df_barber = pd.read_csv(barber_file_path)

    # Concatenate the dfs  
    df_aes['Industry'] = 'Appearance Enhancement Service'
    df_barber['Industry'] = 'Barber'
    df = pd.concat([df_aes,df_barber], axis=0, join='outer', ignore_index=False)

    df = df.rename(columns={"License Number": "Record ID", 'Name':'Business Name 2', 'County':'City', 'License State':'State' ,'Phone':'Contact Phone', 'License Issue Date':'LIC Issue Date', 'Current Term Effective Date':'LIC Current Issue Date', 'Expiration Date':'LIC Exp Date', 'License Status':'LIC Status'})
    
    df["Record ID"] = df["Record ID"].apply(lambda x : x.replace(" ",""))
    df["Record ID"] = df["Record ID"].astype(str)
    df["Business Name 2"] = df["Business Name 2"].astype(str)

    df["Business Name 2"]=df["Business Name 2"].apply(lambda x:x.replace(u'\xa0', u' '))
    df["Address"]=df["Address"].apply(lambda x:x.replace(u'\xa0', u' '))

    df["Address"] = df["Address"].apply(lambda x : x.split("                "))

    df["Building Number"] = ""
    df["Street"] = ""
    df["Zip"] = ""

    df = df.apply(lambda row : clean_addr(row), axis=1)

    del df["Address"]
    del df["Agency"]

    df = source.type_cast(df)
    df = source.clean_zip_city(df)
    df = df.drop_duplicates()
    
    return df

def begin_process(segment):
    source = SourceFile()

    if 0 in segment:
        df = instantiate_file(source)
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/df-doa.p", "wb" ))

    if 1 in segment:
        df = pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/df-doa.p", "rb" ))
        df = source.add_bbl_async(df)
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/df-doa-1.p", "wb" ))

    cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/licenses.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
            
if __name__ == '__main__':
    begin_process([0])