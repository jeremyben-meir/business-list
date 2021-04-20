#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

global counter
counter = 0

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
    application_98_21file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/License_Applications_98_21.csv"
    application_00_12file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/License_Applications_00_12.csv"
    
    df_application_98_21 = pd.read_csv(application_98_21file_path)
    df_application_00_12 = pd.read_csv(application_00_12file_path)

    ##### DATE FORMATTING FOR 00_12 FILE AS STRING AND THEN CONVERSION INTO DATETIME BEFORE FILE MERGE

    df_application_00_12['End Date'] = df_application_00_12['End Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_application_00_12['Start Date'] = df_application_00_12['Start Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_application_00_12['Status Date'] = df_application_00_12['Status Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_application_00_12['License Start'] = df_application_00_12['License Start'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_application_00_12['Expiration Date'] = df_application_00_12['Expiration Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    
    df_application = pd.concat([df_application_98_21,df_application_00_12], ignore_index=True)

    og = ['118','124','101','49','100','9','50','94','78','34','5','86','18','6','64','73','98','107','109','80','125','46','122','75','123','1','33','127','24','13','15','87','21','110','16','66','120','91','102','62','14']
    replace = ['Scrap Metal Processor - 118','Tow Truck Company - 124','Home Improvement Salesperson - 101','Amusement Device Temporary','Garage - 049','Home Improvement Contractor - 100','Parking Lot - 050','General Vendor - 094','Sightseeing Bus - 078','Employment Agency - 034','Secondhand Dealer Auto - 005','Horse Drawn Cab Driver - 086','Amusement Device (Portable) - 018','Secondhand Dealer [General] - 006','Laundry - 064','Cabaret - 073','Garage & Parking Lot - 098','Scale Dealer/Repairer - 107','Process Server (Organization) - 109','Pawnbroker - 080','Tow Truck Driver - 125','Pool or Billiard Room - 046','Debt Collection Agency - 122','Catering Establishment - 075','Motion Picture Projectionist - 123','Electronic Store - 001','Stoop Line Stand - 033','Cigarette Retail Dealer - 127','Newsstand - 024','Sidewalk Cafe - 013','Electronic & Home Appliance Service Dealer - 115','Horse Drawn Cab Owner - 087','Sightseeing Guide - 021','Process Server (Individual) - 110','Amusement Device (Permanent) - 016','Laundry Jobber - 066','Storage Warehouse - 120','Commercial Lessor (Bingo/Games Of Chance) - 091','Special Sale - 102','Locksmith - 062','Amusement Arcade - 014']
    df_application_00_12["Industry"] = df_application_00_12["Industry"].replace(og,replace)

    df_application_00_12['End Date'] = df_application_00_12['End Date'].astype('datetime64[D]')
    df_application_00_12['Start Date'] = df_application_00_12['Start Date'].astype('datetime64[D]')
    df_application_00_12['Status Date'] = df_application_00_12['Status Date'].astype('datetime64[D]')
    df_application_00_12['License Start'] = df_application_00_12['License Start'].astype('datetime64[D]')
    df_application_00_12['Expiration Date'] = df_application_00_12['Expiration Date'].astype('datetime64[D]')

    df_application['Business Name'] = df_application['Business Name'].astype(str)
    df_application['Contact Phone'] = df_application['Contact Phone'].astype(str).apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ","").replace(".","").replace("/","").replace("\\",""))
    df_application['License Number'] = df_application['License Number'].astype(str)

    df_application['License Type'] = df_application['License Type'].astype(str)
    df_application['Application ID'] = df_application['Application ID'].astype(str)
    df_application['Application or Renewal'] = df_application['Application or Renewal'].astype(str)
    df_application['Temp Op Letter Issued'] = df_application['Temp Op Letter Issued'].astype('datetime64[D]')
    df_application['Temp Op Letter Expiration'] = df_application['Temp Op Letter Expiration'].astype('datetime64[D]')
    df_application['Application Category'] = df_application['Application Category'].astype(str)
    df_application['Unit Type'] = df_application['Unit Type'].astype(str)
    df_application['Zip'] = df_application['Zip'].astype(str)
    df_application['Building Number'] = df_application['Building Number'].astype(str)
    df_application['Street'] = df_application['Street'].astype(str)

    df_application = df_application.drop_duplicates(subset=['City', 'Street', 'Building Number', 'Start Date', 'End Date', 'Business Name', 'License Category'], keep='first')

    df_application['BBL'] = np.nan

    df_application["City"] = df_application["City"].astype(str)
    df_application["City"] = df_application["City"].fillna("")
    df_application = df_application[(df_application['City'] == "") | (df_application['City'].isin(nyc_city))]
    
    df_application["Zip"] = df_application["Zip"].astype(str)
    df_application["Zip"] = df_application["Zip"].fillna("")
    df_application["Zip"] = df_application["Zip"].apply(lambda x: str(int(x)) if str(x).isdigit() else "")
    df_application = df_application[(df_application['Zip'] == "") | (df_application['Zip'].isin(manhattan_zips + non_manhattan_zips))]

    del df_application["Application Category"]
    del df_application["Last Update Date"]
    del df_application["Longitude"]
    del df_application["Latitude"]

    df_application = df_application.rename(columns={"License Number": "Record ID", "Expiration Date": "License Expiration Date", "License Category": "Industry", "Start Date": "APP Start Date", "End Date": "APP End Date", "Status": "APP Status", "Status Date": "APP Status Date", "License Start": "License Start Date"})

    pickle.dump(df_application, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-1.p", "wb" ))
    return df_application

def process_1(df_1):
    totlen=len(df_1)
    df_1 = df_1.apply(lambda row: add_bbl(row, totlen=totlen), axis=1)

    pickle.dump(df_1, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "wb" ))
    return df_1

def begin_process():
    df_0 = process_0()
    # # df_0 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-1.p", "rb" ))
    
    df_1 = process_1(df_0)
    # df_1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "rb" ))

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/application_updated_0.csv"
    df_1.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
if __name__ == '__main__':
    begin_process()