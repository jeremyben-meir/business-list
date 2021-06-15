from classes.common import DirectoryFields
from classes.bbl_adder import BBLAdder
import json
import requests
from fuzzywuzzy import fuzz
import pickle
import pandas as pd
import csv
import asyncio
import aiohttp
from multiprocessing import Process
import time
import numpy as np
import concurrent.futures

class SourceFile:

    def format_keys(self):
        val = []
        for row in open(DirectoryFields.LOCAL_LOCUS_PATH + "data/api_keys.txt").readlines():
            currow = row.strip("\n")
            val.append(currow.split(" "))
        return val

    # INIT #################################################################################################

    def __init__(self):
        self.bbl_segment_size = 10000 # These vars effect processing speed
        self.bbl_thread_count = 12 # These vars effect processing speed
        self.keylist = self.format_keys()[:self.bbl_thread_count]

    # ADD BBL ##############################################################################################

    def add_bbl_async(self, df, overwrite=True):
        df = df.reset_index(drop=True)
        dflist = np.array_split(df,len(self.keylist))
        ticker = 0
        adder_objs = []
        for keyset in self.keylist: 
            adder_objs.append(BBLAdder(keyset[1],keyset[0],dflist[ticker], len(self.keylist) - ticker - 1, overwrite, self.bbl_segment_size))
            ticker += 1

        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for adder_obj in adder_objs:
                futures.append(executor.submit(adder_obj.add_bbl_starter))

        resdf = pd.concat([f.result() for f in futures])
        return resdf

    # CITY SETTING #########################################################################################

    def type_cast(self, df):

        df['BBL'] = ""
        df['Record ID'] = df['Record ID'].astype(str)
        df['Contact Phone'] = df['Contact Phone'].astype(str).apply(lambda x: x.replace("-","").replace(")","").replace("(","").replace(" ","").replace(".","").replace("/","").replace("\\",""))
        df['Business Name'] = df['Business Name'].astype(str)
        df['Building Number'] = df['Building Number'].astype(str)
        df['Street'] = df['Street'].astype(str)
        df['Industry'] = df['Industry'].astype(str)

        df['State'] = df['State'].astype(str)    
        df['State'] = df['State'].str.upper()
        df['State'] = df['State'].replace(['N/A','NULL','nan','NaN'],'')
        df['State'] = df['State'].fillna("")
        
        df["Zip"] = df["Zip"].astype(str)
        df['Zip'] = df['Zip'].replace(['N/A','NULL','nan','NaN'],'')
        df["Zip"] = df["Zip"].fillna("")
        df["Zip"] = df["Zip"].apply(lambda x: x.strip(' '))
        df["Zip"] = df["Zip"].apply(lambda x: str(int(float(x))) if x!='' and str(x).isdigit and len(str(int(float(x))))==5 else '')
        
        df["City"] = df["City"].astype(str)
        df['City'] = df['City'].str.upper()
        df['City'] = df['City'].replace(['N/A','NULL','nan','NaN'],'')
        df["City"] = df["City"].fillna("")

        return df

    def clean_zip_city(self, df): #input df must have have header 'City',Zip', and 'State'

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

        df = df[~ ((df['City'].apply(lev_city) > 90) | ((df['City'] =='') & (df['Zip']=='')))]
        df = df.apply(lambda row : row_fix(row), axis=1) 
        df = df[~ (df['City'] == "scheduled_for_deletion")]

        df = df.reset_index(drop=True)
        
        return df