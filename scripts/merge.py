####### IMPORTS #######

from scripts.common import DirectoryFields
from fuzzywuzzy import fuzz
import pickle
import re
import pandas as pd
import uuid
import csv

class Merge():

    food = {'Restaurant-Sandwiches', 'FPE-J,A,V, , , ', 'CATERER\'S PERMIT', 'Restaurant-Pakistani','SUMMER RESTAURANT LIQUOR' ,'FPE-J,H,K,A,C, ', 'Restaurant-Barbecue', 'Restaurant-Delicatessen', 'FPE-J,A,C, , , ', 'Restaurant-Brazilian', 'FPE-J,B,C,H, , ','FPE-J,A,B,C,D,H', 'Restaurant-Soul Food', 'Sidewalk Cafe','Electronic Cigarette Dealer','Restaurant-Armenian','FPE-J,A,C,D,E, ', 'FPE-J,K,D,B,C, ', 'Restaurant-Seafood','FPE-J,A,C,H,K, ', 'FPE-B, , , , , ','FPE-J,C,N,D,K, ', 'WINERY','DISTRIBUTOR “A”', 'Restaurant-Caribbean', 'FPE-A, , , , , ', 'FPE-J,B,C,D,H, ', 'FPE-J,A,H,K, , ','FISHING VESSEL','Tobacco Retail Dealer','FPE-J,D,A,C, , ', 'FPE-J,K,A,C, , ', 'FPE-J,A,D, , , ', 'FPE-J,A,D,C, , ', 'Pool or Billiard Room - 046','VENDOR','Hotel/Motel - 460', 'FPE-J,A,D,H, , ','FPE-J,C,D,E,H,K','ON-PREMISES LIQUOR','FPE-J,C,A, , , ','Restaurant-American', 'Restaurant-Sandwiches/Salads/Mixed Buffet', 'FPE-J,E,K,W, , ','WHOLESALE LIQUOR', 'Restaurant-Chilean', 'FPE-J,A,C,I, , ', 'FPE-J,A,C,G, , ', 'Restaurant-Salads','Restaurant-Café/Coffee/Tea', 'Restaurant-Portuguese', 'FPE-J,A,B,C,H,K', 'Restaurant-Peruvian', 'SUMMER CLUB LIQUOR','Megastore - 821','Restaurant-Polish', 'Restaurant-Afghan', 'Restaurant-Juice, Smoothies, Fruit Salads', 'FPE-J,A,D,X, , ', 'BOTTLE CLUB', 'IMPORTER','Restaurant-Scandinavian', 'FPE-J,B,H,K, , ','Restaurant-Thai', 'TAVERN WINE', 'Restaurant-Chinese', 'Cabaret - 073', 'Restaurant-Hotdogs', 'FPE-J,C,D,K, , ', 'FPE-J,H,K,A,B,C', 'Sidewalk Cafe - 013','Restaurant-Southeast Asian', 'FPE-J,H,A,C, , ', 'Restaurant-Pizza/Italian', 'CATERING ESTABLISHMENT', 'COMBINED CRAFT MANUFACTURER', 'WINERY / FARM WINERY RETAIL', 'MICRO BREWER', 'Restaurant-Filipino', 'DISTILLER "A"', 'FPE-J,B,D,H,K, ','GROCERY BEER, WINE PROD', 'Wholesale Food Market - 718', 'FPE-J,A,B,N, , ','FPE-J,C,H,K, , ', 'FPE-J,A,C,B, , ','FPE-H, , , , , ','Restaurant-Bakery', 'WINTER RESTAURANT WINE', 'VESSEL LIQUOR', 'Supermarket - 819', 'Restaurant-Pizza', 'Restaurant-Ice Cream, Gelato, Yogurt, Ices','FPE-J,A,C,U, , ','FPE-J,K,D,B, , ','FPE-J,K,D,C, , ', 'CABARET LIQUOR ','Cabaret', 'Restaurant-Spanish', 'FPE-J,A,C,D,E,K','Restaurant-Bottled Beverages', 'Restaurant-Polynesian', 'Restaurant-Greek', 'Restaurant-Not Listed/Not Applicable', 'Catering Establishment - 075','Restaurant-Frozen Desserts', 'FPE-J,A,C,D,H,K', 'Restaurant-Cajun','SUMMER RESTAURANT WINE', 'BREWER RETAIL','Restaurant-Asian', 'FPE-J,B,C,D,K, ', 'SUMMER (0. P.) ENTERTAINMENT', 'Spray Paint Sls Mnor - 832', 'FPE-J,A,C,D, , ', 'FPE-J,D,E,H,K, ', 'LIQUOR - NO FEE', 'Restaurant-Fruits/Vegetables',  'VESSEL BEER','FPE-J,D,A, , , ', 'Restaurant-NULL','Restaurant-Czech', 'H25', 'Restaurant-Egyptian', 'FPE-J,C,D,E,K, ','FPE-J,D,C,E, , ', "Tobacco Prod'T Sales - 890" ,'FPE-J,C,A,B, , ', 'FPE-J,A,C,D,E,W', 'FPE-J,B,H, , , ','Cigarette Retail Dealer - 127', 'Restaurant-Vegan', 'Restaurant-Irish', 'FPE-J,C,K, , , ','FPE-J,A,C,H, , ','TEMPORARY WINERY/FARM WINERY', 'Restaurant-Indonesian', 'FPE-J,A,M, , , ','Restaurant-Hamburgers', 'FPE-J,H,D,A,C, ','FPE-J,K,D,A, , ', 'FPE-J,A,C,D,N,K', 'FPE-nan,nan,nan,nan,nan,nan', 'Restaurant-Japanese', 'Restaurant-Indian', 'Restaurant-Italian', 'WHOLESALE BEER(CO)','Restaurant-Lebanese', 'CIDER PRODUCER/WHOLESALER', 'FPE-J,B,C,D,H,K', 'FPE-J,B,D, , , ', 'Restaurant-Pancakes/Waffles', 'FPE-J,A,C,D,K, ','FPE-J,A,B,D,H, ','WINTER (O.P.) FOOD & BEV','FPE-J,C,W, , , ','WHOLESALE CIDER','FPE-J,C,D,H, , ','FPE-J,K,A, , , ', 'FPE-J,K,B, , , ','Restaurant-Chinese/Cuban', 'Restaurant-Donuts','Restaurant-Soups', 'FPE-J,H,D,C,A, ', 'FPE-J,C,N,D, , ','FPE-J,C,E, , , ', 'Restaurant-Creole', 'Restaurant-New American', 'Restaurant-French', 'FPE-J,C,D,K,W, ', 'FPE-J,A,C,C, , ','EATING PLACE BEER','FPE-J,A,B,C,D, ',  'Restaurant - 818','Restaurant-Bakery Products/Desserts', 'FPE-J,A,K,C, , ', 'RESTAURANT LIQUOR', 'FPE-E, , , , , ', 'WINTER EATING PLACE BEER', 'CLUB BEER', 'FPE-J,H,A,B, , ', 'FPE-J,B,C,D, , ',  'Restaurant-Southwestern', 'Restaurant-Californian','Restaurant-African','Restaurant-Korean', 'Restaurant-English','Restaurant-Bangladeshi', 'Restaurant-Hawaiian','Restaurant-Soups & Sandwiches', 'Restaurant-Australian', 'FPE-J,A,E,H,K, ', 'FPE-J,A,B,D, , ', 'FPE-J,D,H,K, , ','Restaurant-Continental','Stoop Line Stand', 'FPE-J,A,B, , , ','SUMMER TAVERN WINE', 'Restaurant-Mediterranean', 'FPE-J,B,C,A, , ','MANUFACTURER', 'Mobile Food Vendor - 881', 'FPE-J,H,K,C, , ','FPE-J,C,D, , , ', 'FPE-J,A,B,C,H, ','FPE-J,D,B, , , ', 'WHOLESALE BEER(C)', 'BREWER', 'Restaurant-Vegetarian', 'FPE-J,H,D,C, , ','FPE-J,C,D,N, , ','FPE-J,C,D,E,H, ', 'Restaurant-Steak', 'FPE-J,A,C,E, , ', 'Restaurant-Bagels/Pretzels', 'FPE-J,A,C,D,H, ', 'FPE-J,H,K,A,B, ', 'FARM BREWER', 'FPE-J,A,C,R, , ', 'RESTAURANT BREWER', 'FPE-C, , , , , ', 'Restaurant-Chinese/Japanese','Catering Establishment',  'Restaurant-Latin (Cuban, Dominican, Puerto Rican, South & Central American)','FPE-J,B,D,K, , ', 'FPE-J,A,C,K, , ', 'Restaurant-Vietnamese/Cambodian/Malaysia', 'WINE PRODUCT(AW)', 'O.P. FOOD AND BEV', 'FPE-J,H,K,B, , ', 'FPE-J,A,B,C,D,K', 'FPE-J,A,N, , , ', 'FARM WINERY', 'Restaurant-Latin American', 'Restaurant-Asian/Asian Fusion', 'FPE-J,H,K,D,C, ', 'FPE-J,A,C,Z, , ', 'FPE-J,D,B,C, , ', 'HOTEL WINE', 'CIDER PRODUCER RETAIL', 'Gaming Cafe - 129','Restaurant-Turkish', 'FPE-J,A,B,H,K, ', 'Restaurant-Soups/Salads/Sandwiches', 'Grocery-Retail - 808', 'FPE-J,C,H, , , ', 'Restaurant-Eastern European', 'DISTILLER "B"', 'FPE-J,A,K, , , ', 'WINE STORE',  'OUTSOURCE FACILITY', 'FPE-J,D,E,K, , ', 'SUMMER O.P. FOOD & BEV', 'Restaurant-Hotdogs/Pretzels', 'FPE-J,C,E,H, , ', 'Restaurant-', 'FARM CIDERY', 'FPE-J,K,C, , , ', 'Restaurant-Coffee/Tea', 'FPE-J,A,D,O, , ', 'FPE-J,B,C,H,K, ',  'SUMMER GOLF CLUB', 'Restaurant-Iranian', 'FPE-J,K,D,A,C, ', 'FPE-J,C,D,H,K, ', 'FPE-J,C,D,S, , ', 'GROCERY STORE BEER','Restaurant-Tex-Mex','TEMPORARY PERMIT',  'FPE-J,B,C,K, , ', 'CUSTOM WINEMAKERS CENTER', 'Restaurant-Nuts/Confectionary', 'SUMMER EATING PLACE BEER', 'FARM DISTILLER "D"', 'Restaurant-Chicken', 'FPE-J,A,B,K, , ', 'Restaurant-Middle Eastern', 'FPE-J,K,A,B,C, ', 'FPE-J,E,H,K, , ', 'O.P. ENTERTAINMENT','FPE-J,A,B,H, , ', 'Restaurant-Ethiopian', 'FPE-J,C,K,H, , ', 'CLUB LIQUOR', 'FPE-J,C,D,E, , ', 'Restaurant-German', 'RESTAURANT WINE', 'Restaurant-Jewish/Kosher', 'Stoop Line Stand - 033', 'FPE-J,B,K, , , ', 'WINTER TAVERN WINE', 'FPE-J,H,A,B,C, ', 'Gaming Cafe', 'FPE-J,K,B,C, , ', 'Restaurant-Mexican', 'Restaurant-Creole/Cajun', 'FPE-J,A,D,K, , ', 'FPE-J,A,B,C,K, ', 'Restaurant-Tapas', 'WHOLESALE WINE',  'Restaurant-Other', 'Restaurant-Moroccan',  'FPE-J,H,C, , , ', 'FPE-J,C,D,K,X, ', 'Restaurant-Russian', 'CLUB WINE', 'FPE-J,K,A,B, , ', 'FPE-J,B,C,K,H, ', 'FPE-J,D,C,K, , ', 'FPE-J,C,D,K,N, ', 'FPE-J,A,D,H,K, ',  'FPE-J,A,O, , , ', 'Restaurant-Steakhouse',  'LIQUOR STORE',  'FPE-J,H,K,D,A,C', 'FPE-J,D,C, , , ', 'Internet Complaints - 443',  'FPE-J,A,B,C, , ', 'Pool or Billiard Room', 'FPE-J,A,Z, , , ',  'FPE-J,E,K, , , ', 'FPE-J,H,K, , , ',  'HOTEL LIQUOR', 'WINE CATERING', 'FPE-J,H,B, , , ',}
    aes = {'aes','Health Spa - 839','barber', 'Salons And Barbershop - 841', 'Internet Complaints - 443',}
    retail = {'Electronic Cigarette Dealer','Locksmith','Hardware-Retail - 811', 'General Vendor Distribution - 097','Tobacco Retail Dealer', 'Pool or Billiard Room - 046', 'Special Sale - 102', 'Dealer In Products For The Disabled - 119', 'Floor Coverings - 241','Pharmacy','Gas Station-Retail - 815','FPE-J,B,C, , , ','Air Condtioning Law - 899','Wearing Apparel - 450', 'DRUG STORE BEER','Electronic & Appliance Service','Misc Non-Food Retail - 817','Secondhand Dealer [General] - 006', 'Spray Paint Sls Mnor - 832','LIQUOR - NO FEE',"Tobacco Prod'T Sales - 890", "Gov'T Agency Retail - 824",'Cigarette Retail Dealer - 127', 'Locksmith Apprentice','Furniture Sales - 242','Gift Certificate - 895','Laser Pointer Sales - 834','Secondhand Dealer - Firearm - 006A','General Vendor Distributor','Drug Wholesaler','Retail Store - 820','Jewelry Store-Retail - 823', 'Electronics Store','Dealer In Products', 'Home Improvement Contractor - 100','Locksmith - 062',  'Newsstand','Home Improvement Contractor','Restaurant-Basque','Stoop Line Stand',  'Electronic & Home Appliance Service Dealer - 115','Pawnbroker','Appliances - 244', 'Special Sale', 'General Vendor - 094','Dealer in Products for the Disabled - 119','Home Improvement Salesperson','General Vendor', 'Drug Store Retail - 810','Secondhand Dealer - General','Restaurant-Bottled beverages, including water, sodas, juices, etc.', 'Pool Or Billiard Room - 046', 'FPE-J,D,H, , , ', '420', 'Stoop Line Stand - 033',  'Electronic Store - 001', 'Newsstand - 024',  'DRUG BEER, WINE PROD', 'Secondhand Dealer - Firearms', 'Scrap Metal Processor', 'Locksmith Apprentice - 063', 'Internet Complaints - 443',}
    laundry = {'Laundry Jobber - 066','Air Condtioning Law - 899', 'Laundry Jobber',  'Laundries', 'Dry Cleaners - 230', 'Internet Complaints - 443',  'Laundry',  'Laundry - 064'}
    cars = {'Parking Lot - 050','Garage', 'Garage & Parking Lot - 098','Gas Station-Retail - 815','Air Condtioning Law - 899', 'Auto Leasing - 211','Booting Company - 126','Booting Company', 'Spray Paint Sls Mnor - 832', 'Tow Truck Company','Tow Truck Driver','Fuel Oil Dealer - 814', 'Tow Truck Driver - 125', 'Garage - 049', 'Car Wash','Garage and Parking Lot','Auto Dealership - 212', 'Gasoline Truck-Retail - 822',  'Tow Truck Company - 124', 'Parking Lot',  'Secondhand Dealer - Auto','Secondhand Dealer Auto - 005',  'Auto Rental - 213', 'Internet Complaints - 443',}
    misc = {'Temporary Street Fair Vendor Permit - 111','Tax Preparers - 891','Employment Agency - 034','H99','Amusement Device (Portable) - 018','9','H05','Auction House - 128', 'H86','23','Horse Drawn Cab Owner','Photography Services - 415','Process Server (Individual) - 110', '4','Bingo Game Operator','H06','Air Condtioning Law - 899','873','3','Games Of Chance - 088','Debt Collection Agency','Horse Drawn Cab Owner - 087', 'Ticket Seller Business', 'Amusement Device Temporary','H03', 'Commercial Lessor (Bingo/Games Of Chance) - 091', 'SD6', 'H29','115','E75','Scale Dealer Repairer','Bingo Game Operator - 089','Sightseeing Guide','Imitation Gun - 836','Process Server Individual', 'Pawnbroker - 080','Mini-Storage Company - 830','Auctioneer - 036','H92', 'H98','Funeral Homes - 888','H27', 'Commercial Lessor','H15','Games of Chance', 'SUMMER (0. P.) ENTERTAINMENT','Horse Drawn Cab Driver - 086',  'Spray Paint Sls Mnor - 832','LIQUOR - NO FEE','Ticket Seller','Pregnancy Service Center (PSC)','Motion Picture Operator - 123','Sightseeing Bus','Process Server (Organization) - 109','Distress Prop Consultants - 247','Sightseeing Guide - 021','H70','Motion Picture Projectionist - 123','WINTER (O.P.) FOOD & BEV','Gift Certificate - 895','Home Improvement Salesperson - 101', 'Debt Settlement - 248', 'Process Serving Agency','H90','Misc Archived', '53','Amusement Arcade - 014', 'Amusement Device Permanent','Mailorder Misc - 319', '119','72','Box Cutter - 831','Auction House Premises', 'I &','71','Amusement Arcade', 'Employment Agency', 'nan',  'Auctioneer','H26','Amusement Device (Permanent) - 016','372', 'Bail Bonds','Amusement Device Portable','Pedicab Business','Sightseeing Bus - 078','PERMIT CLONE FOLDER RECORD', 'Temporary Street Fair Vendor', 'Debt Collection Agency - 122',  'Tenant Screening - 480',  'Tow Truck Exemption', '114', 'Pedicab Driver - 131', '117',  'RAILROAD CAR', 'Other', 'C73', 'H10', 'H01', 'Tickets-Live Perf - 260',  'Travel Agency - 440', 'Pedicab Business - 130', '36', 'Amusement Device (Temporary) - 090',  'Storage Warehouse - 120', 'AIRLINE COMPANY', 'SUMMER VESSEL LIQUOR', 'Storage Warehouse',  'Immigration Svc Prv - 893', 'Motion Picture Projectionist',  'Scrap Metal Processor - 118', 'Pedicab Driver', 'Internet Complaints - 443',  'H33',  'Horse Drawn Driver', 'Scale Dealer/Repairer - 107', 'BALL PARK BEER'}
    dictlist = [food,aes,retail,laundry,cars,misc]

    def __init__(self):
        self.df = self.load_source_files()

    def text_prepare(self, text, street0, street1):
        street0 = street0.lower()
        street1 = street1.lower()
        STOPWORDS = ["nan","corp", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon','smoke','pizza','of','the', 'retail', 'new', 'news', 'food', 'group']
        if len(street0) > 2:
            STOPWORDS += street0.split(" ")
        if len(street1) > 2 :
            STOPWORDS += street1.split(" ")
        text = re.sub(re.compile('[\n\"\'/(){}[]|@,;.#]'), '', text)
        text = re.sub(' +', ' ', text)
        text = text.lower()
        text = ' '.join([word for word in text.split() if word not in STOPWORDS]) 
        text = text.strip()
        return text

    def names_to_match(self, name0, name1, street0, street1):
        return fuzz.WRatio(self.text_prepare(name0,street0,street1),self.text_prepare(name1,street0,street1))

    def phones_to_match(self, phone0, phone1):
        return phone0 == 0 and len(phone0)>6 #and len(phone0) >= 10 and phone0 != "nan" and phone0 != "NaN" and not phone0.isna()

    def industry_to_match(self, industry1, industry2):
        return any([(industry1 in x and industry2 in x) for x in self.dictlist])

    ########################################################################

    def load_source_files(self):
        filelist = [("dca","charge"),("dca","inspection"),("dca","application"),("dca","license"),("doa","inspection"),
                    ("doe","pharmacy"),("doh","inspection"),("dos","license"),("liq","license")]
        df_list = [pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{res[0]}/temp/df-{res[1]}-source.p", "rb" )) for res in filelist]

        df = pd.concat(df_list, axis=0, join='outer', ignore_index=False)
        
        df["LLID"] = ""
        df["LBID"] = ""
        
        self.store_pickle(df,0)
        print("load done")

        print(set(df["Industry"].unique()))
        print()
        print(df.columns.tolist())

        return df

    ########################################################################

    def find_llid_sets(self, index0, row0, group, indexlist):
        indexlist.append(index0)
        for index1,row1 in group.iterrows():
            if index1 not in indexlist:
                if row0["Record ID"] == row1["Record ID"] or row0["Business Name"] == row1["Business Name"] or self.phones_to_match(row0["Contact Phone"],row1["Contact Phone"]) or (self.names_to_match(row0["Business Name"],row1["Business Name"],row0["Street"],row1["Street"]) > 80 and self.industry_to_match(row0['Industry'],row1['Industry'])):
                    indexlist = self.find_llid_sets(index1,row1,group,indexlist)
        return indexlist

    def add_llid(self, bbl):
        self.df = self.df.reset_index(drop=True)

        bblmask = (self.df['BBL'].str.len() == 10) & (self.df['BBL']!="0000000000")
        addrmask = (self.df['Building Number'].str.len() > 0) & (self.df['Building Number']!= 'nan') & (self.df['Street'].str.len() > 0)  & (self.df['Street']!= 'nan') & (self.df['City'].str.len() > 0) & (self.df['City'] != 'nan')
        curgrp = self.df[bblmask].groupby('BBL') if bbl else self.df[~bblmask & addrmask].groupby(['Building Number','Street','City'])

        for _ , group in curgrp:
            indexlist = []
            for index0,row0 in group.iterrows():
                if index0 not in indexlist:
                    indexlist += self.find_llid_sets(index0, row0, group, indexlist)
            self.df.loc[indexlist,"LLID"] = str(uuid.uuid4()) 

        print(f"added LLIDs on {'BBL' if bbl else 'address'}")
        return

    ########################################################################

    def find_lbid_sets_llid(self, index0, row0, indexlist):
        curlist = []
        group = self.df[self.df['LLID'] == row0['LLID']]
        for index1,row1 in group.iterrows():
            curlist.append(index1)
        for index1,row1 in group.iterrows():
            if index1 not in indexlist:
                indexlist = self.find_lbid_sets_RecID(index1,row1,indexlist+curlist)
        return indexlist

    def find_lbid_sets_RecID(self, index0, row0, indexlist):
        curlist = []
        group = self.df[self.df['Record ID'] == row0['Record ID']]
        for index1,row1 in group.iterrows():
            curlist.append(index1)
        for index1,row1 in group.iterrows():
            if index1 not in indexlist:
                indexlist = self.find_lbid_sets_llid(index1,row1,indexlist+curlist)
        return indexlist

    def add_lbid(self):
        self.df = self.df[self.df['LLID'].str.len() > 0]
        self.df = self.df[self.df['Record ID'].str.len() > 5]
        self.df = self.df.reset_index(drop=True)

        curgrp = self.df[self.df['LLID'].str.len() > 0].groupby('LLID')

        for _ , group in curgrp:
            indexlist = []
            for index0,row0 in group.iterrows():
                indexlist.append(index0)
            for index0,row0 in group.iterrows():
                indexlist += self.find_lbid_sets_RecID(index0, row0, indexlist)
            self.df.loc[indexlist,"LBID"] = str(uuid.uuid4()) 
        
        print("added LBIDS")
        return

    ########################################################################

    def store_pickle(self,df,num):
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-{num}.p", "wb" ))
    
    def load_pickle(self,num):
        return pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-{num}.p", "rb" ))

    def save_csv(self):
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/merged.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        self.store_pickle(self.df,"merged")

if __name__ == '__main__':
    merge = Merge()
    # merge.add_llid(bbl=True)
    # merge.add_llid(bbl=False)
    # merge.add_lbid()
    # merge.save_csv()

    