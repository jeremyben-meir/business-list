####### IMPORTS #######

from scripts.common import DirectoryFields
from fuzzywuzzy import fuzz
import pickle
import re
import pandas as pd
import uuid
import csv

class Merge():

    dictlist = []
    dictlist.append({'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', '3', 'Laundry Jobber', 'Car Wash', 'Tenant Screening - 480', 'H92', 'H29', 'H70', 'Hotel/Motel - 460', 'H26', 'Dry Cleaners - 230', 'H25', 'H27','873', '117', '4', 'C73', '72', '119','H98', '71', 'Laundry Jobber - 066', 'SD6', 'H33', 'H03', 'H90', '9', 'Laundry - 064', 'Special Sale - 102', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Laundry', 'Misc Archived', 'Other', 'H01', 'H15', '23', 'Laundries', 'Mobile Food Vendor - 881', '114', 'H06', 'H99', 'Box Cutter - 831', 'Floor Coverings - 241', 'Gasoline Truck-Retail - 822', 'Special Sale', '36', '115', 'Air Condtioning Law - 899', 'H05', 'Internet Complaints - 443', 'E75', '420'})
    dictlist.append({'Restaurant-37', 'Restaurant-35', 'Restaurant-23', 'Restaurant-74', 'Restaurant-76', 'Restaurant-00', 'Restaurant-60', 'Restaurant-19', 'Restaurant-11', 'Restaurant-66', 'Restaurant-61', 'Restaurant-77', 'Restaurant-73', 'Restaurant-51', 'Restaurant-03', 'Restaurant-29', 'Restaurant-70', 'Restaurant-13', 'Restaurant-05', 'Restaurant-57', 'Restaurant-41', 'Restaurant-75', 'Restaurant-NULL', 'Restaurant-67', 'Restaurant-27', 'Restaurant-43', 'Restaurant-21', 'Restaurant-24', 'Restaurant-15', 'Restaurant-64', 'Restaurant-14', 'Restaurant-50', 'Restaurant-55', 'Restaurant-06', 'Restaurant-28', 'Restaurant-33', 'Restaurant-42', 'Restaurant-45', 'Restaurant-47', 'Restaurant-62', 'Restaurant-56', 'Restaurant-81', 'Restaurant-07', 'Restaurant-44', 'Restaurant-80', 'Restaurant-12', 'Restaurant-99', 'Restaurant-59', 'Restaurant-04', 'Restaurant-83', 'Restaurant-84', 'Restaurant-65', 'Restaurant-53', 'Restaurant-39', 'Restaurant-68', 'Restaurant-25', 'Restaurant-72', 'Restaurant-52', 'Restaurant-16', 'Restaurant-63', 'Restaurant-01', 'Restaurant-02', 'Restaurant-78', 'Restaurant-71', 'Restaurant-10', 'Restaurant-32', 'Restaurant-18', 'Restaurant-69', 'Restaurant-20', 'Restaurant-22', 'Restaurant-54', 'Restaurant-36', 'Restaurant-38', 'Restaurant-82', 'Restaurant-31', 'Restaurant-58', 'Restaurant-49', 'Restaurant-26', 'Restaurant-17', 'Restaurant-30', 'Restaurant-48', 'Restaurant-09', 'Restaurant-40', 'Restaurant-46', 'Restaurant-08', 'Restaurant-34',"Tobacco Prod'T Sales - 890", 'Restaurant - 818', 'Supermarket - 819', 'Games of Chance', 'Temporary Street Fair Vendor', 'Amusement Arcade', 'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', 'Commercial Lessor (Bingo/Games Of Chance) - 091', '3', 'H92', 'H29', 'H70', 'Amusement Device (Portable) - 018', 'Hotel/Motel - 460', 'H26', 'Pool Or Billiard Room - 046', 'H25', 'General Vendor Distributor', 'H27', 'Sidewalk Cafe', 'Stoop Line Stand - 033', 'Bingo Game Operator', '873', '117', '4', 'C73', '72', 'Cabaret', '119', 'Catering Establishment', 'General Vendor Distribution - 097', 'H98', '71', 'Grocery-Retail - 808', 'Newsstand', 'Newsstand - 024', 'SD6', 'H33', 'H03', 'H90', 'Gaming Cafe - 129', 'Games Of Chance - 088',  '9', 'Special Sale - 102', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Wholesale Food Market - 718', 'Other', 'H01', 'Amusement Arcade - 014', 'Gaming Cafe', 'Secondhand Dealer - Firearms', 'Cabaret - 073', 'Pool or Billiard Room - 046', 'Temporary Street Fair Vendor Permit - 111', 'Gift Certificate - 895', 'H15', 'General Vendor - 094', 'Cigarette Retail Dealer - 127', 'Catering Establishment - 075', '23', 'Drug Store Retail - 810', 'Mobile Food Vendor - 881', 'Tobacco Retail Dealer', '114', 'H06', 'H99', 'General Vendor', 'Sidewalk Cafe - 013', 'Box Cutter - 831', 'Floor Coverings - 241', 'Stoop Line Stand', 'Electronic Cigarette Dealer', 'Special Sale', '36', '115', 'Air Condtioning Law - 899', 'H05', 'Megastore - 821', 'Pool or Billiard Room', 'Internet Complaints - 443', 'E75', '420'})
    dictlist.append({"Tobacco Prod'T Sales - 890", 'Immigration Svc Prv - 893', 'Games of Chance', 'Travel Agency - 440', 'Temporary Street Fair Vendor', 'Amusement Arcade', 'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', 'Commercial Lessor (Bingo/Games Of Chance) - 091', '3', 'Tenant Screening - 480', 'Bingo Game Operator - 089', 'H92', 'H29', 'Motion Picture Projectionist - 123', 'Fuel Oil Dealer - 814', 'H70', 'Amusement Device (Portable) - 018', 'Hotel/Motel - 460', 'H26', 'Auction House Premises', 'Pool Or Billiard Room - 046', 'H25', 'General Vendor Distributor', 'H27', 'Secondhand Dealer - General', 'Bingo Game Operator', 'Ticket Seller Business', 'Debt Collection Agency - 122', '873', 'Auctioneer', 'Motion Picture Projectionist', '117', 'Secondhand Dealer [General] - 006', 'Bail Bonds', 'Scale Dealer Repairer', 'Auctioneer - 036', '4', 'C73', '72', 'Scale Dealer/Repairer - 107', '119', 'Funeral Homes - 888', 'Pawnbroker - 080', 'Commercial Lessor', 'General Vendor Distribution - 097', 'Secondhand Dealer - Firearm - 006A', 'Process Serving Agency', 'H98', '71', 'Home Improvement Contractor', 'Process Server (Organization) - 109', 'Locksmith Apprentice', 'Pedicab Business - 130', 'Newsstand', 'Ticket Seller', 'Distress Prop Consultants - 247', 'Amusement Device (Temporary) - 090', 'Newsstand - 024', 'Electronic & Appliance Service', 'Imitation Gun - 836', 'SD6', 'H33', 'H03', 'H90', 'Gaming Cafe - 129', 'Games Of Chance - 088', 'Locksmith Apprentice - 063', 'Booting Company - 126', 'Pedicab Driver', '9', 'Motion Picture Operator - 123', 'Special Sale - 102', 'Booting Company', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Gas Station-Retail - 815', 'Pedicab Business', 'Tax Preparers - 891', 'Sightseeing Bus', 'Other', 'Electronic Store - 001', 'Hardware-Retail - 811', 'Home Improvement Salesperson - 101', 'Auction House - 128', 'H01', 'Tow Truck Company', 'Amusement Arcade - 014', 'Electronic & Home Appliance Service Dealer - 115', 'Locksmith', 'Debt Collection Agency', 'Gaming Cafe', 'Secondhand Dealer - Firearms', 'Amusement Device (Permanent) - 016', 'Pool or Billiard Room - 046', 'Amusement Device Portable', 'Furniture Sales - 242', 'Home Improvement Salesperson', 'Temporary Street Fair Vendor Permit - 111', 'Gift Certificate - 895', 'H15', 'General Vendor - 094', 'Locksmith - 062', 'Cigarette Retail Dealer - 127', 'Sightseeing Bus - 078', 'Scrap Metal Processor', 'Storage Warehouse', 'Horse Drawn Cab Driver - 086', 'Amusement Device Permanent', '23','Scrap Metal Processor - 118', 'Photography Services - 415', 'Retail Store - 820', 'Dealer In Products For The Disabled - 119', 'Drug Store Retail - 810', 'Tobacco Retail Dealer', 'Process Server Individual', '114', 'H06', 'Home Improvement Contractor - 100', 'Tickets-Live Perf - 260', 'Amusement Device Temporary', 'H99', 'General Vendor', 'Storage Warehouse - 120', 'Laser Pointer Sales - 834', 'Wearing Apparel - 450', 'Box Cutter - 831', 'Horse Drawn Cab Owner - 087', 'Floor Coverings - 241', 'Dealer in Products for the Disabled - 119', 'Pawnbroker','Gasoline Truck-Retail - 822', 'Process Server (Individual) - 110', 'Sightseeing Guide - 021', 'Auto Leasing - 211', 'Electronic Cigarette Dealer', 'Special Sale', '36', '115', 'Mini-Storage Company - 830', 'Misc Non-Food Retail - 817', 'Air Condtioning Law - 899', 'H05', 'Employment Agency', 'Appliances - 244', 'Megastore - 821', 'Sightseeing Guide', 'Pool or Billiard Room', 'Pregnancy Service Center (PSC)', 'Debt Settlement - 248', 'Employment Agency - 034', 'Internet Complaints - 443', 'Jewelry Store-Retail - 823', 'Electronics Store', 'Dealer In Products', 'E75', '420'})
    dictlist.append({'Horse Drawn Driver','Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &',  '3','Car Wash', 'H92', 'H29', 'Fuel Oil Dealer - 814', 'H70', 'H26', 'H27', '873', '117', '4', 'Tow Truck Driver - 125', 'C73', '72', '119', 'H98', '71', 'Pedicab Business - 130', 'Auto Rental - 213', 'Horse Drawn Cab Owner', 'Secondhand Dealer - Auto', 'SD6', 'H33', 'H03', 'H90', 'Booting Company - 126', 'Pedicab Driver', '9', 'Special Sale - 102', 'Booting Company', 'H86', '53', 'Parking Lot', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Gas Station-Retail - 815', 'Pedicab Business', 'Sightseeing Bus', 'Other', 'Garage & Parking Lot - 098', 'H01', 'Tow Truck Company', 'Gift Certificate - 895', 'H15', 'Pedicab Driver - 131', 'General Vendor - 094', 'Scrap Metal Processor', 'Storage Warehouse', 'Horse Drawn Cab Driver - 086', 'Tow Truck Driver', '23', 'Parking Lot - 050', 'Garage - 049', 'Scrap Metal Processor - 118', 'Secondhand Dealer Auto - 005', 'Auto Dealership - 212', '114', 'H06', 'Garage and Parking Lot',  'H99', 'Storage Warehouse - 120', 'Box Cutter - 831', 'Floor Coverings - 241', 'Gasoline Truck-Retail - 822', 'Auto Leasing - 211', 'Special Sale', '36', '115', 'Mini-Storage Company - 830',  'Air Condtioning Law - 899', 'H05', 'Garage', 'Internet Complaints - 443', 'Tow Truck Company - 124', 'E75', '420'})
    dictlist.append({'Temporary Street Fair Vendor', 'Spray Paint Sls Mnor - 832', "Gov'T Agency Retail - 824", 'I &', '3', 'Tenant Screening - 480', 'H92', 'H29', 'H70', 'Hotel/Motel - 460', 'H26', 'H25', 'H27', '873', '117', '4', 'C73', '72', '119', 'H98', '71', 'SD6', 'H33', 'H03', 'H90', '9', 'Special Sale - 102', 'Tow Truck Exemption', 'H86', '53', 'Health Spa - 839', 'Mailorder Misc - 319', 'H10', 'Misc Archived', 'Other','H01', 'Gift Certificate - 895', 'H15', '23', 'Salons And Barbershop - 841','114', 'H06', 'H99', 'Box Cutter - 831', 'Floor Coverings - 241', 'Special Sale', '36', '115',  'Air Condtioning Law - 899', 'H05', 'Employment Agency', 'Internet Complaints - 443', 'E75', '420'})

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
                    ("doe","pharmacy"),("doh","inspection"),("dos","license"),("liq","liquor")]
        df_list = [pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{res[0]}/temp/df-{res[1]}-source.p", "rb" )) for res in filelist]

        df = pd.concat(df_list, axis=0, join='outer', ignore_index=False)
        
        df["LLID"] = ""
        df["LBID"] = ""
        
        self.store_pickle(df,0)
        print("load done")

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
    merge.add_llid(bbl=True)
    # merge.add_llid(bbl=False)
    merge.add_lbid()
    merge.save_csv()

    