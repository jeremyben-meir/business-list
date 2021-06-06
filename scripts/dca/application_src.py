#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    application_98_21file_path = LOCAL_LOCUS_PATH + "data/dca/License_Applications_98-21.csv"
    application_00_12file_path = LOCAL_LOCUS_PATH + "data/dca/License_Applications_00-12.csv"
    
    df_98_21 = pd.read_csv(application_98_21file_path)
    df_00_12 = pd.read_csv(application_00_12file_path)

    ##### DATE FORMATTING FOR 00_12 FILE AS STRING AND THEN CONVERSION INTO DATETIME BEFORE FILE MERGE

    df_00_12['End Date'] = df_00_12['End Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_00_12['Start Date'] = df_00_12['Start Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_00_12['Status Date'] = df_00_12['Status Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_00_12['License Start'] = df_00_12['License Start'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    df_00_12['Expiration Date'] = df_00_12['Expiration Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
    
    df = pd.concat([df_98_21,df_00_12], ignore_index=True)

    df['End Date'] = df['End Date'].astype('datetime64[D]')
    df['Start Date'] = df['Start Date'].astype('datetime64[D]')
    df['Status Date'] = df['Status Date'].astype('datetime64[D]')
    df['License Start'] = df['License Start'].astype('datetime64[D]')
    df['Expiration Date'] = df['Expiration Date'].astype('datetime64[D]')

    df['Business Name'] = df['Business Name'].astype(str)
    df['Contact Phone'] = df['Contact Phone'].astype(str).apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ","").replace(".","").replace("/","").replace("\\",""))
    df['License Number'] = df['License Number'].astype(str)

    df['License Type'] = df['License Type'].astype(str)
    df['License Category'] = df['License Category'].astype(str)
    df['Application ID'] = df['Application ID'].astype(str)
    df['Application or Renewal'] = df['Application or Renewal'].astype(str)
    df['Temp Op Letter Issued'] = df['Temp Op Letter Issued'].astype('datetime64[D]')
    df['Temp Op Letter Expiration'] = df['Temp Op Letter Expiration'].astype('datetime64[D]')
    df['Unit Type'] = df['Unit Type'].astype(str)
    df['Unit'] = df['Unit'].astype(str)
    df['Description'] = df['Description'].astype(str)
    df['Building Number'] = df['Building Number'].astype(str)
    df['Street'] = df['Street'].astype(str)
    df['Street 2'] = df['Street 2'].astype(str)
    
    global_counter_init(len(df))
    df = clean_zip_city(df)

    del df["Application Category"]
    del df["Last Update Date"]
    del df["Longitude"]
    del df["Latitude"]

    df = df.rename(columns={"License Number": "Record ID", "Expiration Date": "License Expiration Date", "License Category": "Industry", "Start Date": "APP Start Date", "End Date": "APP End Date", "Status": "APP Status", "Status Date": "APP Status Date", "License Start": "License Start Date"})

    og = ['118','124','101','49','100','9','50','94','78','34','5','86','18','6','64','73','98','107','109','80','125','46','122','75','123','1','33','127','24','13','15','87','21','110','16','66','120','91','102','62','14']
    replace = ['Scrap Metal Processor - 118','Tow Truck Company - 124','Home Improvement Salesperson - 101','Amusement Device Temporary','Garage - 049','Home Improvement Contractor - 100','Parking Lot - 050','General Vendor - 094','Sightseeing Bus - 078','Employment Agency - 034','Secondhand Dealer Auto - 005','Horse Drawn Cab Driver - 086','Amusement Device (Portable) - 018','Secondhand Dealer [General] - 006','Laundry - 064','Cabaret - 073','Garage & Parking Lot - 098','Scale Dealer/Repairer - 107','Process Server (Organization) - 109','Pawnbroker - 080','Tow Truck Driver - 125','Pool or Billiard Room - 046','Debt Collection Agency - 122','Catering Establishment - 075','Motion Picture Projectionist - 123','Electronic Store - 001','Stoop Line Stand - 033','Cigarette Retail Dealer - 127','Newsstand - 024','Sidewalk Cafe - 013','Electronic & Home Appliance Service Dealer - 115','Horse Drawn Cab Owner - 087','Sightseeing Guide - 021','Process Server (Individual) - 110','Amusement Device (Permanent) - 016','Laundry Jobber - 066','Storage Warehouse - 120','Commercial Lessor (Bingo/Games Of Chance) - 091','Special Sale - 102','Locksmith - 062','Amusement Arcade - 014']
    df["Industry"] = df["Industry"].replace(og,replace)

    df['BBL'] = ""
    df = df.drop_duplicates()

    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-app.p", "wb" ))

def process_1():
    df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-app.p", "rb" ))
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row), axis=1)
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-app-1.p", "wb" ))

def process_2():
    df = pickle.load(open(LOCAL_LOCUS_PATH + "data/dca/temp/df-app-1.p", "rb" ))
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row, overwrite=False), axis=1)
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-app-1.p", "wb" ))
    df.to_csv(LOCAL_LOCUS_PATH + "data/dca/temp/df-app.csv")
        
if __name__ == '__main__':
    process_2()