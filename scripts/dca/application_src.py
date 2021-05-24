#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########

def process_0():
    application_98_21file_path = LOCAL_LOCUS_PATH + "data/dca/License_Applications_98-21.csv"
    application_00_12file_path = LOCAL_LOCUS_PATH + "data/dca/License_Applications_00-12.csv"
    
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

    return df_application

def begin_process():
    df = process_0()
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/dca/temp/df-app.p", "wb" ))
        
if __name__ == '__main__':
    begin_process()