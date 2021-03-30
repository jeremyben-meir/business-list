



#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########
def add_bbl(row):
    stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
    stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]
    
    row['Address'][0].replace(",","")
    if not any(x.isdigit() for x in row['Address'][0]):
        del row['Address'][0]
    
    for word in stopwords1:
        if word in row['Address'][0].upper():
            row['Address'][0] = row['Address'][0][:row['Address'][0].upper().index(word)]

    for word in stopwords:
        if word in row['Address'][0].upper():
            mylist = row['Address'][0].split(" ")
            for x in range(0, len(mylist)):
                if mylist[x].upper() in stopwords:
                    mylist = mylist[:x]
                    break
            row['Address'][0] = " ".join(mylist)            

    try:
        if row['Address'][0].split(" ")[1][:-1].isnumeric() or row['Address'][0].split(" ")[1].isnumeric():
            mylist = row['Address'][0].split(" ")
            mylist[1] = mylist[0] + "-" + mylist[1]
            del mylist[0]
            row['Address'][0] = " ".join(mylist)
            # print(" ".join(mylist))
    except:
        pass
        
    inject = row['Address'][0] + " " + " ".join(row['Address'][1].split(" ")[: -2])
    try:
        response = requests.get("https://api.cityofnewyork.us/geoclient/v1/search.json?input="+ inject +"&app_id=d4aa601d&app_key=f75348e4baa7754836afb55dc9b6363d")
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        row["BBL"]=json_loaded['results'][0]['response']['bbl']
        print("BBL " + str(json_loaded['results'][0]['response']['bbl']))
    except:
        print(inject)
        row["BBL"]=""

    return row

def begin_process():
    barber_file_path = LOCAL_LOCUS_PATH + "data/whole/aca_license_center/barber.csv"
    aes_file_path = LOCAL_LOCUS_PATH + "data/whole/aca_license_center/aes.csv"
    barber_df = pd.read_csv(barber_file_path, encoding='utf-8')
    aes_df = pd.read_csv(aes_file_path, encoding='utf-8')
    mydf = pd.concat([barber_df,aes_df])

    mydf["License Number"] = mydf["License Number"].apply(lambda x : x.replace(" ",""))
    mydf["License Number"] = mydf["License Number"].astype(str)
    mydf["Name"] = mydf["Name"].astype(str)

    mydf["Name"]=mydf["Name"].apply(lambda x:x.replace(u'\xa0', u' '))
    mydf["Address"]=mydf["Address"].apply(lambda x:x.replace(u'\xa0', u' '))

    mydf["Address"] = mydf["Address"].apply(lambda x : x.split("                "))

    print(mydf["Address"])

    mydf = mydf.apply(lambda x : add_bbl(x), axis=1)
# 
    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/aca_license_center/barber_aes_update_0.csv"
    mydf.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    begin_process()