#######IMPORTS#######

from global_vars import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import uuid

from collections import Counter

#######FUNCTION DEFINITIONS#########
global dictlist
dictlist = []
dictlist.append({'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', '3', 'Laundry Jobber', 'Car Wash', 'Tenant Screening - 480', 'H92', 'H29', 'H70', 'Hotel/Motel - 460', 'H26', 'Dry Cleaners - 230', 'H25', 'H27','873', '117', '4', 'C73', '72', '119','H98', '71', 'Laundry Jobber - 066', 'SD6', 'H33', 'H03', 'H90', '9', 'Laundry - 064', 'Special Sale - 102', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Laundry', 'Misc Archived', 'Other', 'H01', 'H15', '23', 'Laundries', 'Mobile Food Vendor - 881', '114', 'H06', 'H99', 'Box Cutter - 831', 'Floor Coverings - 241', 'Gasoline Truck-Retail - 822', 'Special Sale', '36', '115', 'Air Condtioning Law - 899', 'H05', 'Internet Complaints - 443', 'E75', '420'})
dictlist.append({"Tobacco Prod'T Sales - 890", 'Restaurant - 818', 'Supermarket - 819', 'Games of Chance', 'Temporary Street Fair Vendor', 'Amusement Arcade', 'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', 'Commercial Lessor (Bingo/Games Of Chance) - 091', '3', 'H92', 'H29', 'H70', 'Amusement Device (Portable) - 018', 'Hotel/Motel - 460', 'H26', 'Pool Or Billiard Room - 046', 'H25', 'General Vendor Distributor', 'H27', 'Sidewalk Cafe', 'Stoop Line Stand - 033', 'Bingo Game Operator', '873', '117', '4', 'C73', '72', 'Cabaret', '119', 'Catering Establishment', 'General Vendor Distribution - 097', 'H98', '71', 'Grocery-Retail - 808', 'Newsstand', 'Newsstand - 024', 'SD6', 'H33', 'H03', 'H90', 'Gaming Cafe - 129', 'Games Of Chance - 088',  '9', 'Special Sale - 102', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Wholesale Food Market - 718', 'Other', 'H01', 'Amusement Arcade - 014', 'Gaming Cafe', 'Secondhand Dealer - Firearms', 'Cabaret - 073', 'Pool or Billiard Room - 046', 'Temporary Street Fair Vendor Permit - 111', 'Gift Certificate - 895', 'H15', 'General Vendor - 094', 'Cigarette Retail Dealer - 127', 'Catering Establishment - 075', '23', 'Drug Store Retail - 810', 'Mobile Food Vendor - 881', 'Tobacco Retail Dealer', '114', 'H06', 'H99', 'General Vendor', 'Sidewalk Cafe - 013', 'Box Cutter - 831', 'Floor Coverings - 241', 'Stoop Line Stand', 'Electronic Cigarette Dealer', 'Special Sale', '36', '115', 'Air Condtioning Law - 899', 'H05', 'Megastore - 821', 'Pool or Billiard Room', 'Internet Complaints - 443', 'E75', '420'})
dictlist.append({"Tobacco Prod'T Sales - 890", 'Immigration Svc Prv - 893', 'Horse Drawn Driver', 'Games of Chance', 'Travel Agency - 440', 'Temporary Street Fair Vendor', 'Amusement Arcade', 'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', 'Commercial Lessor (Bingo/Games Of Chance) - 091', '3','Car Wash', 'Tenant Screening - 480', 'Bingo Game Operator - 089', 'H92', 'H29', 'Motion Picture Projectionist - 123', 'Fuel Oil Dealer - 814', 'H70', 'Amusement Device (Portable) - 018', 'Hotel/Motel - 460', 'H26', 'Auction House Premises', 'Pool Or Billiard Room - 046', 'H25', 'General Vendor Distributor', 'H27', 'Secondhand Dealer - General', 'Bingo Game Operator', 'Ticket Seller Business', 'Debt Collection Agency - 122', '873', 'Auctioneer', 'Motion Picture Projectionist', '117', 'Secondhand Dealer [General] - 006', 'Bail Bonds', 'Scale Dealer Repairer', 'Auctioneer - 036', '4', 'Tow Truck Driver - 125', 'C73', '72', 'Scale Dealer/Repairer - 107', '119', 'Funeral Homes - 888', 'Pawnbroker - 080', 'Commercial Lessor', 'General Vendor Distribution - 097', 'Secondhand Dealer - Firearm - 006A', 'Process Serving Agency', 'H98', '71', 'Home Improvement Contractor', 'Process Server (Organization) - 109', 'Locksmith Apprentice', 'Pedicab Business - 130', 'Newsstand', 'Ticket Seller', 'Auto Rental - 213', 'Distress Prop Consultants - 247', 'Horse Drawn Cab Owner', 'Amusement Device (Temporary) - 090', 'Secondhand Dealer - Auto', 'Newsstand - 024', 'Electronic & Appliance Service', 'Imitation Gun - 836', 'SD6', 'H33', 'H03', 'H90', 'Gaming Cafe - 129', 'Games Of Chance - 088', 'Locksmith Apprentice - 063', 'Booting Company - 126', 'Pedicab Driver', '9', 'Motion Picture Operator - 123', 'Special Sale - 102', 'Tow Truck Exemption', 'Booting Company', 'H86', '53', 'Parking Lot', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Gas Station-Retail - 815', 'Pedicab Business', 'Tax Preparers - 891', 'Sightseeing Bus', 'Other', 'Electronic Store - 001', 'Garage & Parking Lot - 098', 'Hardware-Retail - 811', 'Home Improvement Salesperson - 101', 'Auction House - 128', 'H01', 'Tow Truck Company', 'Amusement Arcade - 014', 'Electronic & Home Appliance Service Dealer - 115', 'Locksmith', 'Debt Collection Agency', 'Gaming Cafe', 'Secondhand Dealer - Firearms', 'Amusement Device (Permanent) - 016', 'Pool or Billiard Room - 046', 'Amusement Device Portable', 'Furniture Sales - 242', 'Home Improvement Salesperson', 'Temporary Street Fair Vendor Permit - 111', 'Gift Certificate - 895', 'H15', 'Pedicab Driver - 131', 'General Vendor - 094', 'Locksmith - 062', 'Cigarette Retail Dealer - 127', 'Catering Establishment - 075', 'Sightseeing Bus - 078', 'Scrap Metal Processor', 'Storage Warehouse', 'Horse Drawn Cab Driver - 086', 'Tow Truck Driver', 'Amusement Device Permanent', '23', 'Parking Lot - 050', 'Garage - 049', 'Scrap Metal Processor - 118', 'Photography Services - 415', 'Secondhand Dealer Auto - 005', 'Retail Store - 820', 'Dealer In Products For The Disabled - 119', 'Drug Store Retail - 810', 'Auto Dealership - 212', 'Tobacco Retail Dealer', 'Process Server Individual', '114', 'H06', 'Garage and Parking Lot', 'Home Improvement Contractor - 100', 'Tickets-Live Perf - 260', 'Amusement Device Temporary', 'H99', 'General Vendor', 'Storage Warehouse - 120', 'Laser Pointer Sales - 834', 'Wearing Apparel - 450', 'Box Cutter - 831', 'Horse Drawn Cab Owner - 087', 'Floor Coverings - 241', 'Dealer in Products for the Disabled - 119', 'Pawnbroker','Gasoline Truck-Retail - 822', 'Process Server (Individual) - 110', 'Sightseeing Guide - 021', 'Auto Leasing - 211', 'Electronic Cigarette Dealer', 'Special Sale', '36', '115', 'Mini-Storage Company - 830', 'Misc Non-Food Retail - 817', 'Air Condtioning Law - 899', 'H05', 'Employment Agency', 'Appliances - 244', 'Megastore - 821', 'Sightseeing Guide', 'Pool or Billiard Room', 'Pregnancy Service Center (PSC)', 'Debt Settlement - 248', 'Garage', 'Employment Agency - 034', 'Internet Complaints - 443', 'Jewelry Store-Retail - 823', 'Electronics Store', 'Dealer In Products', 'Tow Truck Company - 124', 'E75', '420'})
dictlist.append({'Temporary Street Fair Vendor', 'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', '3', 'Tenant Screening - 480', 'H92', 'H29', 'H70', 'Hotel/Motel - 460', 'H26', 'H25', 'H27', '873', '117', '4', 'C73', '72', '119', 'H98', '71', 'SD6', 'H33', 'H03', 'H90', '9', 'Special Sale - 102', 'Tow Truck Exemption', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Other','H01', 'Gift Certificate - 895', 'H15', '23', 'Salons And Barbershop - 841','114', 'H06', 'H99', 'Box Cutter - 831', 'Floor Coverings - 241', 'Special Sale', '36', '115',  'Air Condtioning Law - 899', 'H05', 'Employment Agency', 'Internet Complaints - 443', 'E75', '420'})

global counter
counter = 0

def add_bbl(row,totlen):
    # stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
    # stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]       
    global counter
    if len(row['Zip']) != 5:
      inject = row['Building Number'] + " " + row['Street'] + " " + row['City']
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
    if round(counter/totlen,4)>round((counter-1)/totlen,4):
        print(str(round(100*counter/totlen,2)) + "%")
    return row

def text_prepare(text, street0, street1):
    STOPWORDS = ["corp", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon', 'smoke','pizza','of','the', 'new', 'news', 'food', 'group']
    if len(street0) > 2:
        STOPWORDS += street0.split(" ")
    if len(street1) > 2 :
        STOPWORDS += street1.split(" ")
    text = re.sub(re.compile('[\n\"\'/(){}\[\]\|@,;#]'), ' ', text)
    text = re.sub(' +', ' ', text)
    text = text.lower()
    text = ' '.join([word for word in text.split() if word not in STOPWORDS]) 
    text = text.strip()
    return text

def reorder(df):
    df = df[['Record ID', 'Business Name', 'INSP Date', 'Industry', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit','Description', 'City', 'State', 'Zip', 'BBL', 'Application ID','License Type', 'Application or Renewal', 'APP Status','APP Status Date', 'APP Start Date', 'APP End Date','License Start Date', 'License Expiration Date','Temp Op Letter Issued', 'Temp Op Letter Expiration', 'Contact Phone','CHRG Date']]
    return df

def names_to_match(name0, name1, street0, street1):
    return fuzz.WRatio(text_prepare(name0,street0,street1),text_prepare(name1,street0,street1))

def phones_to_match(phone0, phone1):
    return phone0 == 0 and len(phone0)>6 #and len(phone0) >= 10 and phone0 != "nan" and phone0 != "NaN" and not phone0.isna()

def industry_to_match(industry1, industry2):
    global dictlist
    return any([(industry1 in x and industry2 in x) for x in dictlist])

def isNaN(string):
    return string != string

def type_cast(df):
    df['Record ID'] = df['Record ID'].astype(str)
    df['Contact Phone'] = df['Contact Phone'].astype(str)
    df['APP Start Date'] = df['APP Start Date'].astype('datetime64[D]')
    df['APP Status Date'] = df['APP Status Date'].astype('datetime64[D]')
    df['APP End Date'] = df['APP End Date'].astype('datetime64[D]')
    df['License Start Date'] = df['License Start Date'].astype('datetime64[D]')
    df['License Expiration Date'] = df['License Expiration Date'].astype('datetime64[D]')
    df['Temp Op Letter Issued'] = df['Temp Op Letter Issued'].astype('datetime64[D]')
    df['Temp Op Letter Expiration'] = df['Temp Op Letter Expiration'].astype('datetime64[D]')
    df['CHRG Date'] = df['CHRG Date'].astype('datetime64[D]')

    return df

def find_llid_sets(index0, row0, group, indexlist):
    indexlist.append(index0)
    for index1,row1 in group.iterrows():
        if index1 not in indexlist:
            if row0["Record ID"] == row1["Record ID"] or row0["Business Name"] == row1["Business Name"] or phones_to_match(row0["Contact Phone"],row1["Contact Phone"]) or (names_to_match(row0["Business Name"],row1["Business Name"],row0["Street"],row1["Street"]) > 80 and industry_to_match(row0['Industry'],row1['Industry'])):
                indexlist = find_llid_sets(index1,row1,group,indexlist)
    return indexlist
        

def add_llid(df, bbl):
    df = df.reset_index(drop=True)

    bblmask = (df['BBL'].str.len() == 10) & (df['BBL']!="0000000000")
    addrmask = (df['Building Number'].str.len() > 0) & (df['Building Number']!= 'nan') & (df['Street'].str.len() > 0)  & (df['Street']!= 'nan') & (df['City'].str.len() > 0) & (df['City'] != 'nan')
    curgrp = df[bblmask].groupby('BBL') if bbl else df[~bblmask & addrmask].groupby(['Building Number','Street','City'])

    thislen = len(curgrp) ##PRINT
    counter = 0 ##PRINT
    for _ , group in curgrp:
        excludelist = []
        for index0,row0 in group.iterrows():
            indexlist = []
            if index0 not in excludelist:
                indexlist = find_llid_sets(index0, row0, group, indexlist)
                excludelist += indexlist
                df.loc[indexlist,"LLID"] = str(uuid.uuid4()) 

        counter += 1 ##PRINT
        print(float(counter)/thislen) ##PRINT

    return df

def find_lbid_sets_llid(index0, row0, df, indexlist):
    indexlist.append(index0)
    group = df[df['LLID'] == row0['LLID']]
    for index1,row1 in group.iterrows():
        if index1 not in indexlist:
            indexlist = find_lbid_sets_RecID(index1,row1,df,indexlist)
    return indexlist

def find_lbid_sets_RecID(index0, row0, df, indexlist):
    indexlist.append(index0)
    group = df[df['Record ID'] == row0['Record ID']]
    for index1,row1 in group.iterrows():
        if index1 not in indexlist:
            indexlist = find_lbid_sets_llid(index1,row1,df,indexlist)
    return indexlist

def add_lbid(df):
    df = df.reset_index(drop=True)

    curgrp = df[df['LLID'].str.len() > 0].groupby('LLID')

    thislen = len(curgrp) ##PRINT
    counter = 0 ##PRINT
    for _ , group in curgrp:
        excludelist = []
        for index0,row0 in group.iterrows():
            indexlist = []
            if index0 not in indexlist:
                indexlist = find_lbid_sets_RecID(index0, row0, df, indexlist)
                excludelist += indexlist
        df.loc[excludelist,"LBID"] = str(uuid.uuid4()) 

        counter += 1 ##PRINT
        print(float(counter)/thislen) ##PRINT

    return df

def load_source_files():
    df_o1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "rb" ))
    df_o2 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "rb" ))
    df_o3 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "rb" ))

    df = pd.concat([df_o1,df_o2,df_o3], axis=0, join='outer', ignore_index=False)

    df = type_cast(df)

    totlen = len(df)
    df = df.replace(nyc_city,correct_nyc_city)
    df = df.apply(lambda row: add_bbl(row, totlen=totlen), axis=1)

    df["LLID"] = ""
    df["LBID"] = ""
    return df
    
def begin_process(segment):

    if 0 in segment:
        ## LOAD SOURCE FILES
        df = load_source_files()
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-1.p", "wb" ))
        print("load done")

    if 1 in segment:
        ## ADD LLID ON BBL
        df = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-1.p", "rb" ))
        df = add_llid(df, bbl=True)
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-2.p", "wb" ))
        print("added LLIDs on BBL")

    if 2 in segment:
        ## ADD LLID ON ADDRESS
        df = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-2.p", "rb" ))
        df = add_llid(df, bbl=False)
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-3.p", "wb" ))
        print("added LLIDs on address")

    if 3 in segment:
        ## ADD LBID
        df = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-3.p", "rb" ))
        df = df[df['LLID'].str.len() > 0]
        df = df[df['Record ID'].str.len() > 5]
        df = add_lbid(df)
        pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-4.p", "wb" ))
        print("added LBIDS")

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/inspection_application.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)


if __name__ == '__main__':
    begin_process([0,1,2,3])