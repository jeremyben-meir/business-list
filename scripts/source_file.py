from scripts.common import DirectoryFields
from scripts.bbl_adder import BBLAdder
import json
import requests
from fuzzywuzzy import fuzz
import pickle
import pandas as pd
import csv
import re
import sys
import math
import smart_open

class SourceFile:

    def format_keys(self):
        textfile = smart_open.smart_open(f"{DirectoryFields.S3_PATH}data/api_keys.txt")
        val = []
        for line in textfile:
            val.append(line.decode('utf-8').strip("\n").split(" "))
        return val

    # INIT #################################################################################################

    def __init__(self, df, file_manager):
        self.keylist = self.format_keys()
        self.df = df
        self.file_manager = file_manager

    # ADD BBL ##############################################################################################

    def add_bbl_async(self):
        print("Adding BBL")
        self.df = self.file_manager.load_pickle(0)
        self.df = self.df.reset_index(drop=True)
        self.df = BBLAdder(self.df).add_bbl_starter()
        self.file_manager.store_pickle(self.df,1)

    # CITY SETTING #########################################################################################
    
    def stop_end_words(self, row):
        stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
        stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]
        endwords = ["AVE","AVENUE","ST","STREET","ROAD","RD","PLACE","PL","BOULEVARD","BLVD"]
        avenuewords = ['EAST','WEST','SOUTH','NORTH','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','X','Y','Z']
        avenuewords1 = ['OF THE FINEST','OF THE STRONGEST','ST JOHN','ST. JOHN','OF AFRICA','OF ASIA','OF COMMERCE','OF PEACE','OF PROGRESS','OF DISCOVERY','OF RESEARCH','OF THE AMERICAS','OF THE STATES','OF TRANSPORTATION']

        row["Street"] = row["Street"].replace(",","")
        row["Street"] = row["Street"].replace(".","")
        row["Street"] = row["Street"].replace("\"","")
        row["Street"] = row["Street"].replace("-","")
        row["Street"] = row["Street"].strip(" ")
        row["Street"] = row["Street"].upper()

        for word in stopwords1:
            if word in row["Street"]:
                row["Street"] = row["Street"][:row["Street"].index(word)]

        if any(word in row["Street"] for word in stopwords):
            mylist = row["Street"].split(" ")
            for x in range(0, len(mylist)):
                if mylist[x] in stopwords:
                    mylist = mylist[:x]
                    row["Street"] = " ".join(mylist)
                    break
        
        mylist = row["Street"].split(" ")
        for x in range(0, len(mylist)):
            if mylist[x] in endwords:
                if len(mylist)>=x+2 and mylist[x+1] in avenuewords:
                    mylist = mylist[:x+2]
                    row["Street"] = " ".join(mylist)
                elif any(avenueword in " ".join(mylist[x+1:]) for avenueword in avenuewords1):
                    for avenueword in avenuewords1:
                        if avenueword in " ".join(mylist[x+1:]):
                            row["Street"] = " ".join(mylist)
                            row["Street"] = row["Street"][:row["Street"].index(avenueword)+len(avenueword)]
                            break
                else:
                    mylist = mylist[:x+1]
                    row["Street"] = " ".join(mylist)
                break

        return row

    def type_cast(self):
        print("Type casting")
        
        if 'BBL' in self.df.columns.to_list():
            self.df['BBL'] = self.df['BBL'].astype(str)
            self.df['BBL'] = self.df['BBL'].apply(lambda x: '' if math.isnan(float(x.replace('-','').replace('/',''))) else (str(int(float(x.replace('-','').replace('/','')))) if str(int(float(x.replace('-','').replace('/','')))).isdigit() and len(str(int(float(x.replace('-','').replace('/','')))).strip(" "))==10  else ""))
        else:
            self.df['BBL'] = ""

        if 'Longitude' in self.df.columns.to_list() and 'Latitude' in self.df.columns.to_list():
            self.df['Longitude'] = self.df['Longitude'].astype(float)
            self.df['Latitude'] = self.df['Latitude'].astype(float)
        else:
            self.df['Longitude'] = 0.0
            self.df['Latitude'] = 0.0
        
        self.df['Record ID'] = self.df['Record ID'].astype(str)

        self.df['Contact Phone'] = self.df['Contact Phone'].astype(str)
        self.df['Contact Phone'] = self.df['Contact Phone'].apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ","").replace(".","").replace("/","").replace("\\",""))

        self.df['Business Name'] = self.df['Business Name'].astype(str)
        
        self.df['Industry'] = self.df['Industry'].astype(str)

        self.df['State'] = self.df['State'].astype(str)    
        self.df['State'] = self.df['State'].str.upper()
        self.df['State'] = self.df['State'].replace(['N/A','NULL','nan','NaN'],'')
        self.df['State'] = self.df['State'].fillna("")
        
        self.df["Zip"] = self.df["Zip"].astype(str)
        self.df['Zip'] = self.df['Zip'].replace(['N/A','NULL','nan','NaN'],'')
        self.df["Zip"] = self.df["Zip"].fillna("")
        self.df["Zip"] = self.df["Zip"].apply(lambda x: x.strip(' '))
        
        def fix_zip(x):
            try:
                res = str(int(float(x)))
                if len(res)==5:
                    return res
            except:
                return ""
        self.df["Zip"] = self.df["Zip"].apply(lambda x: fix_zip(x))
        
        self.df["City"] = self.df["City"].astype(str)
        self.df['City'] = self.df['City'].str.upper()
        self.df['City'] = self.df['City'].replace(['N/A','NULL','nan','NaN'],'')
        self.df["City"] = self.df["City"].fillna("")

        def fix_dashed(x):
            if (len(x)==6 or len(x)==8) and (x.isnumeric()):
                split_index = round(len(x)/2)
                if abs(int(x[:split_index])-int(x[split_index:])) <= 100:
                    x = f"{x[:split_index]}-{x[split_index:]}"
            return x
        self.df['Building Number'] = self.df['Building Number'].astype(str)
        self.df['Building Number'] = self.df['Building Number'].apply(lambda x: x.strip(' '))
        self.df['Building Number'] = self.df['Building Number'].apply(lambda x: fix_dashed(x))

        self.df['Street'] = self.df['Street'].astype(str)
        self.df['Street'] = self.df['Street'].replace("  "," ")
        self.df['Street'] = self.df['Street'].replace(",","")
        self.df['Street'] = self.df['Street'].apply(lambda x: x.strip(' '))

    def clean_zip_city(self): #input df must have have header 'City',Zip', and 'State'
        print("Cleaning address")

        bronx_zip = ['10465', '10460', '10471', '10474', '10453', '10470', '10472', '10454', '10463', '10451', '10475', '10461', '10469', '10473', '10467', '10457', '10466', '10458', '10468', '10456', '10452', '10459', '10455', '10462', '10464']
        staten_island_zip = ['10310', '10309', '10302', '10312', '10301', '10303', '10306', '10307', '10305', '10311', '10308', '10314', '10304']
        brooklyn_zip = ['11219', '11225', '11207', '11241', '11218', '11216', '11235', '11217', '11220', '11203', '11210', '11237', '11209', '11232', '11224', '11229', '11226', '11238', '11201', '11230', '11231', '11228', '11212', '11234', '11239', '11236', '11221', '11242', '11249', '11233', '11215', '11222', '11206', '11204', '11205', '11211', '11213', '11214', '11243', '11256', '11208', '11252', '11223']
        new_york_zip = ['10175', '10014', '10115', '10039', '10155', '10110', '10170', '10120', '10026', '10005', '10030', '10031', '10024', '10022', '10151', '10112', '10029', '10032', '10019', '10006', '10282', '10027', '10003', '10033', '10009', '10004', '10017', '10098', '10173', '10105', '10106', '10021', '10172', '10020', '10048', '10162', '10034', '10174', '10278', '10023', '10165', '10015', '10028', '10158', '10122', '10060', '10036', '10037', '10118', '10095', '10041', '10153', '10013', '10099', '10012', '10001', '10270', '10018', '10007', '10167', '10281', '10279', '10168', '10016', '10038', '10154', '10166', '10121', '10044', '10152', '10161', '10171', '10280', '10119', '10176', '10025', '10271', '10069', '10199', '10103', '10055', '10177', '10011', '10178', '10040', '10104', '10123', '10002', '10107', '10035', '10111', '10169', '10045', '10090', '10010', '10128']
        queens_zip = ['11385', '11004', '11413', '11369', '11421', '11372', '11427', '11435', '11414', '11432', '11423', '11368', '11426', '11411', '11429', '11433', '11365', '11377', '11419', '11416', '11422', '11364', '11367', '11103', '11420', '11693', '11697', '11417', '11363', '11358', '11102', '11371', '11101', '11360', '11109', '11370', '11362', '11692', '11412', '11104', '11434', '11418', '11361', '11379', '11375', '11356', '11436', '11694', '11354', '11355', '11357', '11106', '11105', '11428', '11359', '11373', '11430', '11366', '11351', '11374', '11378', '11415', '11691']
        nyc_zips = bronx_zip + staten_island_zip + brooklyn_zip + new_york_zip + queens_zip

        not_nyc = ['EAST ROCKAWAY','BRONXVILLE','INWOOD']
        bronx = ['BRONX']
        staten_island = ['STATEN ISLAND']
        brooklyn = ['BROOKLYN','BKLYN']
        new_york = ['NEW YORK','MANHATTAN','ROOSEVELT ISLAND','WARDS ISLAND']
        queens = ['OZONE PARK','HOLLIS','DOUGLASTON','BRIARWOOD','BELLE HARBOR','ARVERNE','QUEENS','ROCKAWAY PARK','ROCKAWAY POINT','ROCKAWAY BEACH','SUNNYSIDE','FLUSHING','BROAD CHANNEL','QUEENS VILLAGE','SOUTH OZONE PARK','RICHMOND HILL','SOUTH RICHMOND HILL','REGO PARK','RIDGEWOOD','ROSEDALE','ST ALBANS','SAINT ALBANS','WHITESTONE','HOLLISWOOD','WOODHAVEN','WOODSIDE','SPRINGFIELD GARDENS','LONG ISLAND CITY','LIC','L.I.C.','HOLLIS HILLS','HOWARD BEACH','JACKSON HEIGHTS','KEW GARDENS HILLS','CAMBRIA HEIGHTS','BELLEROSE','ASTORIA','BAYSIDE','BELLEROSE MANOR','BREEZY POINT','COLLEGE POINT','CORONA','EAST ELMHURST','ELMHURST','FAR ROCKAWAY','FLORAL PARK','FOREST HILLS','FRESH MEADOWS','GLENDALE','GLEN OAKS','JAMAICA','JAMAICA ESTATES','KEW GARDENS','LITTLE NECK','MASPETH','MIDDLE VILLAGE','LAURELTON','OAKLAND GARDENS']
        fuzzy_nyc = bronx + staten_island + brooklyn + new_york + queens

        endwords = ["AVE","AVENUE","ST","STREET","ROAD","RD","PLACE","PL","BOULEVARD","BLVD"]

        def lev_city(st, city_list = not_nyc):
            maxlev = 0
            for i in city_list:
                ratio = fuzz.ratio(st,i) 
                if ratio > maxlev:
                    maxlev = ratio
            return maxlev

        def city_lev(st, city_list = not_nyc):
            maxlev = 0
            maxlevcity = ''
            for i in city_list:
                ratio = fuzz.ratio(st,i) 
                if ratio > maxlev:
                    maxlev = ratio
                    maxlevcity = i
            return maxlevcity
        
        def boro_assign(st):
            if st in bronx:
                return 'BRONX'
            if st in staten_island:
                return 'STATEN ISLAND'
            if st in brooklyn:
                return 'BROOKLYN'
            if st in new_york:
                return 'NEW YORK'
            if st in queens:
                return'QUEENS'

        def zip_assign(st):
            if st in staten_island_zip:
                return 'STATEN ISLAND'
            if st in bronx_zip:
                return 'BRONX'
            if st in brooklyn_zip:
                return 'BROOKLYN'
            if st in queens_zip:
                return 'QUEENS'
            if st in new_york_zip:
                return 'NEW YORK'

        def city_correct(row):
            row["City"] = boro_assign(city_lev(row["City"], fuzzy_nyc))
            if row['Zip'] not in nyc_zips:
                row['Zip'] = ''
            if (row['State']!='NY'):
                row['State'] = 'NY'
            return row

        def zip_correct(row):
            row['City'] = zip_assign(row['Zip'])
            if (row['State']!='NY'):
                row['State'] = 'NY'
            return row

        def row_delete(row):
            row["City"] = "scheduled_for_deletion"
            return row

        def row_fix(row):
            if row['Zip'] in nyc_zips:
                return zip_correct(row)
            if lev_city(row['City'], fuzzy_nyc) > 85:
                return city_correct(row)
            return row_delete(row)

        def address_clean(row):
            mylist = row['Street'].split(" ")
            for x in range(0, len(mylist)):
                if mylist[x].upper() in endwords:
                    mylist = mylist[:x+1]
                    row['Street'] = " ".join(mylist)
                    break
            return row

        self.df = self.df[~ ((self.df['City'].apply(lev_city) > 90) | ((self.df['City'] =='') & (self.df['Zip']=='')))]
        self.df = self.df.apply(lambda row : row_fix(row), axis=1) 
        self.df = self.df.apply(lambda row : address_clean(row), axis=1) 
        self.df = self.df[~ (self.df['City'] == "scheduled_for_deletion")]

        self.df = self.df.reset_index(drop=True)

    def save_csv(self):
        self.df["source"] = f"{self.file_manager.department}/{self.file_manager.outname}"
        self.file_manager.save_csv(self.df)
        self.file_manager.store_pickle(self.df,"source")