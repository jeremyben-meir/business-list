####### IMPORTS #######

from scripts.common import DirectoryFields
from fuzzywuzzy import fuzz
import pickle
import re
import pandas as pd
import uuid
import csv
import recordlinkage
from recordlinkage.base import BaseCompareFeature
import time
import sys
import numpy as np

class Graph:
 
    # init function to declare class variables
    def __init__(self, V):
        self.V = V
        self.adj = [[] for i in range(V)]
 
    def DFSUtil(self, temp, v, visited):
 
        # Mark the current vertex as visited
        visited[v] = True
 
        # Store the vertex to list
        temp.append(v)
 
        # Repeat for all vertices adjacent
        # to this vertex v
        for i in self.adj[v]:
            if visited[i] == False:
 
                # Update the list
                temp = self.DFSUtil(temp, i, visited)
        return temp
 
    # method to add an undirected edge
    def addEdges(self, edges):
        for edge in edges:
            v,w = edge
            self.adj[v].append(w)
            self.adj[w].append(v)
 
    # Method to retrieve connected components
    # in an undirected graph
    def connectedComponents(self):
        visited = []
        cc = []
        for _ in range(self.V):
            visited.append(False)
        for v in range(self.V):
            if visited[v] == False:
                temp = []
                cc.append(self.DFSUtil(temp, v, visited))
        return cc

class CompareIndustries(BaseCompareFeature):

    food = {'Catering Establishment', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Wholesale Manufacturer,Vehicle, ', 'Restaurant-Korean', 'Restaurant-Sandwiches/Salads/Mixed Buffet', 'Restaurant-Australian', 'FPE-Multiple Operations,Store,Food Warehouse,Wholesale Manufacturer, , ', 'FPE-Multiple Operations,Bakery,Food Warehouse,Wholesale Manufacturer,Vehicle, ', 'SUMMER RESTAURANT LIQUOR', 'Restaurant-Bakery Products/Desserts', 'Restaurant-Chinese/Japanese', 'Event Space', 'Restaurant-Continental', 'Gift Certificate - 895', 'FPE-Multiple Operations,Store,Food Warehouse,Vehicle, , ', 'Pool Or Billiard Room - 046', 'Restaurant-Moroccan', 'FPE-Multiple Operations,Store,Wholesale Produce Packer, , , ', 'WINTER EATING PLACE BEER', 'FPE-Multiple Operations,Food Warehouse,Wholesale Manufacturer,Vehicle, , ', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer,Wholesale Manufacturer, ', 'Restaurant-Sandwiches', 'FPE-Multiple Operations,Store,Produce Grower/Packer/Broker,Storage, , , ', 'SUMMER RESTAURANT WINE', 'Restaurant-Soups', 'Restaurant-Bottled beverages, including water, sodas, juices, etc.', 'O.P. FOOD AND BEV', 'IMPORTER', 'DISTILLER "B"', "Tobacco Prod'T Sales - 890", 'FPE-Multiple Operations,Bakery,Food Manufacturer,Vehicle,Wholesale Manufacturer, ', 'SUMMER (0. P.) ENTERTAINMENT', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Wholesale Manufacturer, ', 'Box Cutter - 831', 'Restaurant-Thai', 'Restaurant-Tapas', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Beverage Plant, ', 'VESSEL LIQUOR', 'H25', 'FPE-Multiple Operations,Food Warehouse,Bakery,Food Manufacturer, , ', 'FPE-Multiple Operations,Store,Bakery,Wholesale Manufacturer, , ', 'FPE-Multiple Operations,Food Manufacturer,Vehicle, , , ', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer,Vehicle, ', 'DISTRIBUTOR “A”', 'Restaurant-Spanish', 'Restaurant-New American', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Food Warehouse,Vehicle, ', 'FPE-Multiple Operations,Food Warehouse,Store, , , ', 'FPE-Multiple Operations,Bakery,Vehicle, , , ', 'Restaurant-Bakery', 'FPE-Multiple Operations,Vehicle,Food Manufacturer, , , ', 'Restaurant-Soul Food', 'Restaurant-Not Listed/Not Applicable', 'Restaurant-Polish', 'WHOLESALE WINE', 'MICRO BREWER', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Vehicle, , ', 'FPE-Multiple Operations,Bakery,Wholesale Manufacturer,Vehicle, , ', 'Tobacco Retail Dealer', 'RESTAURANT BREWER', 'Grocery-Retail - 808', 'Gaming Cafe', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Vehicle,X, ', 'FPE-Multiple Operations,Food Warehouse,Food Manufacturer,Beverage Plant, , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Disposal Plant/Transportation Service, , ', 'FPE-Multiple Operations,Bakery,Food Warehouse,Vehicle, , ', 'Restaurant-Californian', 'Restaurant-Ethiopian', 'Restaurant-Steakhouse', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer,Wholesale Manufacturer,Vehicle', 'FPE-Multiple Operations,Beverage Plant,Vehicle, , , ', 'FPE-Multiple Operations,Food Manufacturer,Beverage Plant,Wholesale Manufacturer, , ', 'FPE-Multiple Operations,Vehicle,Food Warehouse,Store, , ', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Food Manufacturer, , ', 'FPE-Multiple Operations,Store,Bakery,Food Warehouse, , ', 'FPE-Multiple Operations,Food Manufacturer,Farm Winery-Exempt 20-C, for OCR Use, , , ', 'Cabaret - 073', 'FARM CIDERY', 'FPE-Multiple Operations,Food Warehouse,Food Manufacturer,Vehicle, , ', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse, , , ', 'FPE-Multiple Operations,Store,Wholesale Manufacturer,Vehicle, , ', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer,Food Warehouse, ', 'O.P. ENTERTAINMENT', 'FPE-Multiple Operations,Store,Bakery,Wholesale Manufacturer,Vehicle, ', 'FPE-Multiple Operations,Vehicle,Store,Bakery,Food Manufacturer, ', 'Restaurant-Portuguese', 'Restaurant-Creole', 'FPE-nan,nan,nan,nan,nan,nan', 'FPE-Multiple Operations,Food Warehouse,Wholesale Manufacturer, , , ', 'Spray Paint Sls Mnor - 832', 'WINE STORE', 'FPE-Multiple Operations,Store,Food Warehouse, , , ', 'FPE-Food Manufacturer, , , , , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Refrigerated Warehouse, , ', 'RESTAURANT WINE', 'TEMPORARY WINERY/FARM WINERY', 'Restaurant-Pizza', 'Restaurant-Bangladeshi', 'HOTEL LIQUOR', 'FPE-Multiple Operations,Wholesale Manufacturer,Food Manufacturer, , , ', 'WINE CATERING', 'Restaurant-Nuts/Confectionary', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Beverage Plant,Vehicle', 'FPE-Multiple Operations,Store,Beverage Plant,Wholesale Manufacturer,Vehicle, ', 'Restaurant-Soups/Salads/Sandwiches', 'Restaurant-French', 'FPE-Multiple Operations,Store,Food Manufacturer,Farm Product Use, , ', 'Mailorder Misc - 319', 'Restaurant-Scandinavian', 'TAVERN WINE', 'ON-PREMISES LIQUOR', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Beverage Plant,Wholesale Manufacturer,Vehicle', 'FPE-Multiple Operations,Vehicle,Food Warehouse,Bakery,Food Manufacturer, ', 'SUMMER TAVERN WINE', 'Restaurant-Peruvian', 'COMBINED CRAFT MANUFACTURER', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Beverage Plant, , ', 'Restaurant-Hotdogs', 'Restaurant-', 'Restaurant-Indian', 'Restaurant-German', 'Restaurant-Fruits/Vegetables', 'FPE-Multiple Operations,Wholesale Manufacturer,Bakery, , , ', 'Restaurant-Salads', 'FPE-Multiple Operations,Food Warehouse,Beverage Plant,Wholesale Manufacturer,Vehicle, ', 'WINE PRODUCT(AW)', "CATERER'S PERMIT", 'FPE-Multiple Operations,Wholesale Manufacturer,Food Warehouse,Store,Food Manufacturer, ', 'TEMPORARY PERMIT', 'Restaurant-Frozen Desserts', 'FPE-Multiple Operations,Store,Food Warehouse,X, , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Manufacturer, , ', 'FPE-Multiple Operations,Store,Food Warehouse,Produce Grower/Packer/Broker,Storage, , ', 'FPE-Multiple Operations,Food Warehouse,Bakery, , , ', 'Restaurant-Chinese', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Store,Food Manufacturer, ', 'WINERY / FARM WINERY RETAIL', 'FPE-Multiple Operations,Beverage Plant,Vehicle,Farm Winery-Exempt 20-C, for OCR Use, , ', 'FPE-Multiple Operations,Store,Food Warehouse,Wholesale Manufacturer,Vehicle, ', 'FPE-Multiple Operations,Store,Vehicle, , , ', 'FPE-Wholesale Manufacturer, , , , , ', 'FARM DISTILLER "D"', 'FARM WINERY', 'FPE-Beverage Plant, , , , , ', 'FPE-Multiple Operations,Food Manufacturer,Beverage Plant, , , ', 'Restaurant-Southeast Asian', 'LIQUOR STORE', 'Mobile Food Vendor - 881', 'CLUB LIQUOR', 'EATING PLACE BEER', 'Restaurant-Asian/Asian Fusion', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Wholesale Manufacturer, , ', 'Restaurant-Other', 'Restaurant-Southwestern', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Food Warehouse,Store,Food Manufacturer', 'Restaurant-Mexican', 'FPE-Multiple Operations,Store,Food Manufacturer,Pet Food Manufacturer, , ', 'Restaurant-Mediterranean', 'Hotel/Motel - 460', 'BOTTLE CLUB', 'Pool or Billiard Room - 046', 'Restaurant - 818', 'CATERING ESTABLISHMENT', 'FPE-Multiple Operations,Food Manufacturer,Wholesale Produce Packer,Food Warehouse,Vehicle, ', 'FPE-Multiple Operations,Bakery,Food Manufacturer, , , ', 'FPE-Multiple Operations,Food Manufacturer,Wholesale Manufacturer, , , ', 'FPE-Multiple Operations,Food Manufacturer,Store, , , ', 'Restaurant-Ice Cream, Gelato, Yogurt, Ices', 'FPE-Multiple Operations,Store,Food Manufacturer,Wholesale Manufacturer, , ', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Vehicle,Wholesale Produce Packer, ', 'DISTILLER "A"', 'FPE-Store, , , , , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Processing Plant, , ', 'Restaurant-Delicatessen', 'VESSEL BEER', 'Restaurant-Lebanese', 'Restaurant-Latin (Cuban, Dominican, Puerto Rican, South & Central American)', 'FPE-Multiple Operations,Food Manufacturer,Vehicle,Wholesale Manufacturer, , ', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle, , , ', 'CIDER PRODUCER RETAIL', 'LIQUOR - NO FEE', 'FPE-Multiple Operations,Food Warehouse,Store,Food Manufacturer, , ', 'Restaurant-Donuts', 'RESTAURANT LIQUOR', 'Restaurant-Vietnamese/Cambodian/Malaysia', 'Internet Complaints - 443', 'Restaurant-Iranian', 'FPE-Multiple Operations,Vehicle,Bakery,Food Manufacturer, , ', 'WHOLESALE CIDER', 'Restaurant-Soups & Sandwiches', 'FPE-Multiple Operations,Store,Bakery, , , ', 'Restaurant-Steak', 'FPE-Multiple Operations,Food Manufacturer,Wholesale Manufacturer,Vehicle, , ', 'Restaurant-Greek', 'FPE-Multiple Operations,Vehicle,Store,Bakery, , ', 'FPE-Multiple Operations,Store,Food Warehouse,Food Manufacturer, , ', 'FPE-Multiple Operations,Wholesale Manufacturer,Store,Bakery,Food Manufacturer, ', 'Other', 'FPE-Multiple Operations,Store,Bakery,Vehicle, , ', 'Restaurant-Tex-Mex', 'Sidewalk Cafe - 013', 'MANUFACTURER', 'Restaurant-English', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer,Food Warehouse,Wholesale Manufacturer', 'Restaurant-Creole/Cajun', 'Restaurant-Jewish/Kosher', 'Restaurant-Vegan', 'FPE-Bakery, , , , , ', 'Restaurant-Bottled Beverages', 'Restaurant-Italian', 'Restaurant-Pakistani', 'PERMIT CLONE FOLDER RECORD', 'Restaurant-Afghan', 'Wholesale Food Market - 718', 'Restaurant-Asian', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Wholesale Produce Packer, , ', 'WINTER RESTAURANT WINE', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Store,Bakery,Food Manufacturer', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Beverage Plant,Wholesale Manufacturer, ', 'Restaurant-Cajun', 'Restaurant-Basque', 'Pool or Billiard Room', 'FPE-Multiple Operations,Store,Food Manufacturer,Bakery, , ', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer, , ', 'FPE-Multiple Operations,Bakery,Food Warehouse, , , ', 'WINTER (O.P.) FOOD & BEV', 'Sidewalk Cafe', 'Restaurant-Egyptian', 'Cabaret', 'Restaurant-Pancakes/Waffles', 'Restaurant-Chilean', 'Restaurant-Armenian', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Wholesale Manufacturer, , ', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Food Warehouse,Food Manufacturer, ', 'Restaurant-Seafood', 'SUMMER O.P. FOOD & BEV', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Wholesale Produce Packer,Vehicle', 'FPE-Multiple Operations,Vehicle,Food Warehouse,Food Manufacturer, , ', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Store, , ', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Vehicle,Farm Winery-Exempt 20-C, for OCR Use, ', 'HOTEL WINE', 'Stoop Line Stand - 033', 'Restaurant-Filipino', 'Cigarette Retail Dealer - 127', 'Restaurant-Brazilian', 'WINTER TAVERN WINE', 'FPE-Multiple Operations,Vehicle,Bakery, , , ', 'Restaurant-Irish', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Vehicle, , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Beverage Plant, , ', 'nan', 'OUTSOURCE FACILITY', 'FPE-Multiple Operations,Store,Food Manufacturer,Wholesale Manufacturer,Vehicle, ', 'FPE-Multiple Operations,Wholesale Manufacturer,Store,Bakery, , ', 'CLUB BEER', 'BREWER', 'Restaurant-Chinese/Cuban', 'FPE-Multiple Operations,Store,Vehicle,Food Manufacturer, , ', 'Restaurant-Eastern European', 'E75', 'Restaurant-Bagels/Pretzels', 'Restaurant-Russian', 'Restaurant-Polynesian', 'FPE-Multiple Operations,Vehicle,Store,Food Manufacturer, , ', 'FPE-Multiple Operations,Food Manufacturer,Store,Bakery, , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Wholesale Manufacturer,Vehicle', 'FARM BREWER', 'Restaurant-Juice, Smoothies, Fruit Salads', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Beverage Plant,Farm Winery-Exempt 20-C, for OCR Use', 'WHOLESALE BEER(CO)', 'FPE-Multiple Operations,Wholesale Manufacturer,Store,Food Manufacturer, , ', 'FPE-Multiple Operations,Vehicle,Store, , , ', 'FPE-Multiple Operations,Wholesale Manufacturer,Food Warehouse,Food Manufacturer,Store, ', 'FPE-Multiple Operations,Bakery,Wholesale Manufacturer, , , ', 'Restaurant-Chicken', 'SUMMER CLUB LIQUOR', 'FPE-Multiple Operations,Beverage Plant,Wholesale Manufacturer,Vehicle, , ', 'Stoop Line Stand', 'FPE-Multiple Operations,Store,Food Manufacturer, , , ', 'CUSTOM WINEMAKERS CENTER', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse, , ', 'FPE-Multiple Operations,Vehicle,Food Warehouse,Bakery, , ', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Food Warehouse,Wholesale Manufacturer, ', 'Restaurant-Turkish', 'CIDER PRODUCER/WHOLESALER', 'Restaurant-Hawaiian', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Store,Bakery, ', 'FISHING VESSEL', 'FPE-Multiple Operations,Store,Salvage Dealer, , , ', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Wholesale Manufacturer,Vehicle, ', 'Restaurant-Latin American', 'Restaurant-Vegetarian', 'CLUB WINE', 'FPE-Multiple Operations,Food Manufacturer,Wholesale Produce Packer,Food Warehouse, , ', 'FPE-Multiple Operations,Store,Food Manufacturer,Food Warehouse,Vehicle, ', 'Restaurant-American', 'Restaurant-Pizza/Italian', 'FPE-Multiple Operations,Store,Bakery,Food Manufacturer,Food Warehouse,Vehicle', 'GROCERY BEER, WINE PROD', 'Restaurant-Middle Eastern', 'Restaurant-Indonesian', 'FPE-Multiple Operations,Food Warehouse,Beverage Plant,Vehicle, , ', 'SUMMER EATING PLACE BEER', 'Restaurant-African', 'Supermarket - 819', 'Restaurant-Hamburgers', 'FPE-Multiple Operations,Store,Bakery,Wholesale Produce Packer, , ', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Food Warehouse, , ', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Feed Warehouse and/or Distributor, , ', 'Restaurant-Japanese', 'Restaurant-Barbecue', 'Misc Archived', 'BREWER RETAIL', 'Electronic Cigarette Dealer', 'FPE-Multiple Operations,Store,Bakery,Food Warehouse,Wholesale Manufacturer, ', 'Restaurant-NULL', 'Restaurant-Hotdogs/Pretzels', 'VENDOR', 'Megastore - 821', 'FPE-Multiple Operations,Store,Farm Product Use, , , ', 'FPE-Multiple Operations,Bakery,Food Manufacturer,Food Warehouse,Wholesale Manufacturer,Vehicle', 'Gaming Cafe - 129', 'Restaurant-Coffee/Tea', 'FPE-Multiple Operations,Food Warehouse,Food Manufacturer, , , ', 'Catering Establishment - 075', 'FPE-Multiple Operations,Store,Slaughterhouse, , , ', 'Restaurant-Caribbean', 'Restaurant-Café/Coffee/Tea', 'Restaurant-Czech', 'WHOLESALE BEER(C)', 'FPE-Multiple Operations,Vehicle,Food Warehouse,Store,Food Manufacturer, ', 'FPE-Multiple Operations,Wholesale Manufacturer,Vehicle,Bakery, , ', 'FPE-Multiple Operations,Wholesale Manufacturer,Food Warehouse,Food Manufacturer, , ', 'WHOLESALE LIQUOR', 'CABARET LIQUOR ', 'FPE-Multiple Operations,Store,Food Manufacturer,Vehicle, , ', 'WINERY', 'SUMMER GOLF CLUB', 'FPE-Multiple Operations,Food Manufacturer,Food Warehouse,Beverage Plant,Vehicle, ', 'GROCERY STORE BEER'}
    aes = {'aes','Health Spa - 839','barber', 'Salons And Barbershop - 841', 'Internet Complaints - 443','Barber','nan','Other','Misc Archived'}
    retail = {'Electronic Cigarette Dealer','Locksmith','Hardware-Retail - 811', 'General Vendor Distribution - 097','Tobacco Retail Dealer','nan','Auction House - 128','Special Sale - 102','Other','Dealer In Products For The Disabled', 'Dealer In Products For The Disabled - 119', 'Floor Coverings - 241','Pharmacy','Gas Station-Retail - 815','Air Condtioning Law - 899','Wearing Apparel - 450', 'DRUG STORE BEER','Electronic & Appliance Service','Misc Non-Food Retail - 817','Secondhand Dealer [General] - 006', 'Spray Paint Sls Mnor - 832','Healthcare',"Tobacco Prod'T Sales - 890", 'H29', "Gov'T Agency Retail - 824",'Cigarette Retail Dealer - 127', 'Pawnbroker - 080', 'Locksmith Apprentice','Furniture Sales - 242','Gift Certificate - 895','Auctioneer - 036','Laser Pointer Sales - 834','Secondhand Dealer - Firearm - 006A','General Vendor Distributor','Drug Wholesaler','Retail Store - 820','Jewelry Store-Retail - 823', 'Electronics Store','Dealer In Products', 'Home Improvement Contractor - 100','Locksmith - 062',  'Newsstand','Home Improvement Contractor','Stoop Line Stand',  'Electronic & Home Appliance Service Dealer - 115','Pawnbroker','Appliances - 244', 'Special Sale', 'General Vendor - 094','Dealer in Products for the Disabled - 119','Misc Archived','Home Improvement Salesperson','General Vendor', 'Mailorder Misc - 319','Box Cutter - 831','Drug Store Retail - 810','Secondhand Dealer - General', 'Shoe Store','Auction House Premises', 'Stoop Line Stand - 033', 'PERMIT CLONE FOLDER RECORD', 'Auctioneer', 'Electronic Store - 001', 'Newsstand - 024',  'DRUG BEER, WINE PROD', 'Secondhand Dealer - Firearms', 'Scrap Metal Processor', 'Locksmith Apprentice - 063', 'Internet Complaints - 443','Electronic & Home Appliance Service Dealer','Other'}
    laundry = {'Laundry Jobber - 066','Air Condtioning Law - 899', 'Laundry Jobber',  'Misc Archived', 'Laundries', 'Dry Cleaners - 230', 'nan','Internet Complaints - 443',  'Laundry',  'Other','Laundry - 064'}
    cars = {'Parking Lot - 050','Garage', 'Garage & Parking Lot - 098','Gas Station-Retail - 815', 'Misc Archived','Air Condtioning Law - 899', 'Auto Leasing - 211','Booting Company - 126','Booting Company', 'Spray Paint Sls Mnor - 832', 'Tow Truck Company','Tow Truck Driver','Fuel Oil Dealer - 814', 'Tow Truck Driver - 125', 'Garage - 049', 'Car Wash','Garage and Parking Lot','Auto Dealership - 212', 'Gasoline Truck-Retail - 822',  'Tow Truck Company - 124', 'Parking Lot',  'Secondhand Dealer - Auto','Secondhand Dealer Auto - 005',  'Auto Rental - 213', 'Internet Complaints - 443','Auto Repair', 'Other','nan','Auto Parts','Spray Paint Sls Mnor - 832', 'Tow Truck Exemption'}
    misc = {'Temporary Street Fair Vendor Permit - 111','Tax Preparers - 891','Employment Agency - 034','Amusement Device (Portable) - 018','9','H05', 'H86','23','Horse Drawn Cab Owner','Photography Services - 415','Process Server (Individual) - 110', '4','Bingo Game Operator','H06','Air Condtioning Law - 899','873','3','Games Of Chance - 088','Debt Collection Agency','Horse Drawn Cab Owner - 087', 'Ticket Seller Business', 'Amusement Device Temporary','H03', 'Commercial Lessor (Bingo/Games Of Chance) - 091',  'Scale Dealer Repairer','Bingo Game Operator - 089','Sightseeing Guide','Imitation Gun - 836','Process Server Individual', 'Mini-Storage Company - 830', 'Funeral Homes - 888', 'Commercial Lessor','H15','Games of Chance','Horse Drawn Cab Driver - 086',  'Spray Paint Sls Mnor - 832','Ticket Seller','Pregnancy Service Center (PSC)','Motion Picture Operator - 123','Sightseeing Bus','Process Server (Organization) - 109','Distress Prop Consultants - 247','Sightseeing Guide - 021','H70','Motion Picture Projectionist - 123','Home Improvement Salesperson - 101', 'Debt Settlement - 248', 'Process Serving Agency','Misc Archived', '53','Amusement Arcade - 014', 'Amusement Device Permanent','72', 'I &','71','Amusement Arcade', 'Employment Agency', 'nan', 'H26','Amusement Device (Permanent) - 016','372', 'Bail Bonds','Amusement Device Portable','Pedicab Business','Sightseeing Bus - 078','PERMIT CLONE FOLDER RECORD', 'Temporary Street Fair Vendor', 'Debt Collection Agency - 122',  'Tenant Screening - 480',   'Pedicab Driver - 131', '117',  'RAILROAD CAR', 'Event Space', 'Other', 'H10', 'H01', 'Tickets-Live Perf - 260',  'Travel Agency - 440', 'Pedicab Business - 130', '36', 'Amusement Device (Temporary) - 090',  'Storage Warehouse - 120', 'AIRLINE COMPANY', 'SUMMER VESSEL LIQUOR', 'Storage Warehouse',  'Immigration Svc Prv - 893', 'Motion Picture Projectionist',  'Scrap Metal Processor - 118', 'Pedicab Driver', 'Internet Complaints - 443',  'H33',  'Horse Drawn Driver', 'Scale Dealer/Repairer - 107', 'BALL PARK BEER'}
    dictlist = [food,aes,retail,laundry,cars,misc]

    def _compute_vectorized(self, industry_0, industry_1):
        mask_list = False
        for x in self.dictlist:
            mask_list |= ((industry_0.isin(x)) & (industry_1.isin(x)))
        return mask_list.astype(float)

class CompareNames(BaseCompareFeature):

    def _compute_vectorized(self, bn0, bn1):
        conc = pd.Series(list(zip(bn0, bn1)))

        def wratio_apply(x):
            return fuzz.WRatio(x[0],x[1])/100.0

        return conc.apply(wratio_apply)

class CompareRecordIDs(BaseCompareFeature):

    def _compute_vectorized(self, record_id_0_0, record_id_0_1, record_id_1_0, record_id_1_1):
        sim = ((((record_id_0_0 == record_id_1_0) | (record_id_0_0 == record_id_1_1)) & (~record_id_0_0.isna())) |
               (((record_id_0_1 == record_id_1_0) | (record_id_0_1 == record_id_1_1)) & (~record_id_0_1.isna()))).astype(float)
        return sim

class ComparePhones(BaseCompareFeature):

    def _compute_vectorized(self, phone_0, phone_1):
        sim = (phone_0 == phone_1).astype(float)
        sim[(sim == 0) & (phone_0.isna()|phone_1.isna())] = 0.5
        return sim
        
class Merge():

    def __init__(self):
        self.df = self.load_source_files()

    def text_prepare(self, text, street0):
        STOPWORDS = ["nan","corp", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon','smoke','pizza','of','the', 'retail', 'new', 'news', 'food', 'group']
        if street0:
            street0 = street0.lower()
            if len(street0) > 2:
                STOPWORDS += street0.split(" ")
        text = re.sub(re.compile('[\n\"\'/(){}[]|@,;.#]'), '', text)
        text = re.sub(' +', ' ', text)
        text = text.lower()
        text = ' '.join([word for word in text.split() if word not in STOPWORDS]) 
        text = text.strip()
        return text

    def prepare_business_names(self,row):
        bn_list = []
        if isinstance(row["Business Name"],str):
            bn_list.append(self.text_prepare(row["Business Name"],row["Street"]))
        if isinstance(row["Business Name 2"],str):
            bn_list.append(self.text_prepare(row["Business Name 2"],row["Street"]))
        row["bn"] = " ".join(bn_list)
        return row

    def load_source_files(self, loaded = True):

        sys.setrecursionlimit(3629)
        print(sys.getrecursionlimit())
        
        if not loaded:
            filelist = [("dca","charge"),("dca","inspection"),("dca","application"),("dca","license"),("doa","inspection"),
                        ("doe","pharmacy"),("doh","inspection"),("dos","license"),("liq","license")]
            df_list = [pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{res[0]}/temp/df-{res[1]}-source.p", "rb" )) for res in filelist]

            df = pd.concat(df_list, axis=0, join='outer', ignore_index=False)
            
            df["LLID"] = ""
            df["LBID"] = ""
            df["Record ID"] = df["Record ID"].astype(str)
            df["Record ID 2"] = df["Record ID 2"].astype(str)
            df["Street"] = df["Street"].astype(str)
            df["Business Name"] = df["Business Name"].astype(str)
            df["Business Name 2"] = df["Business Name 2"].astype(str)
            df["Contact Phone"] = df["Contact Phone"].astype(str)
            df["Industry"] = df["Industry"].astype(str)
            
            # df = df.sort_values(["BBL"])##
            # df = df.iloc[:1000]##

            df = df.apply(lambda row: self.prepare_business_names(row), axis=1)
            bblmask = (df['BBL'].str.len() == 10) & (df['BBL']!="0000000000") & (df['BBL'].str.isdigit())
            df = df[bblmask]
            df = df.drop_duplicates()
            df = df.reset_index(drop=True)
            df = df.replace("", np.nan, regex=True)
            df = df.replace("NaN", np.nan, regex=True)
            df = df.replace("nan", np.nan, regex=True)

            self.store_pickle(df,0)

        else:

            df = self.load_pickle(0)

        print("loaded data")
        return df

    def add_llid(self):

        indexer = recordlinkage.Index()
        indexer.block(on='BBL')
        candidate_links = indexer.index(self.df)

        compare_cl = recordlinkage.Compare()

        compare_cl.add(ComparePhones('Contact Phone','Contact Phone'))
        compare_cl.add(CompareRecordIDs(('Record ID','Record ID 2'),('Record ID','Record ID 2')))
        compare_cl.add(CompareIndustries('Industry','Industry'))
        compare_cl.add(CompareNames('bn','bn'))

        features = compare_cl.compute(candidate_links, self.df)

        print(features)

        # classifier = recordlinkage.ECMClassifier(init="random")
        # probs = classifier.prob(features)
        # print(probs)

        classifier = recordlinkage.KMeansClassifier()
        matches = classifier.fit_predict(features)
        print(matches)

        g = Graph(len(self.df.index))
        g.addEdges(matches.tolist())
        cc = g.connectedComponents()
        
        for indexlist in cc:
            self.df.loc[indexlist,"LLID"] = str(uuid.uuid4())

        del self.df["bn"]

        self.store_pickle(self.df,1)

    def add_lbid(self):
        # self.df.groupby(['LLID','Record ID',"Record ID 2"])
        self.df = self.load_pickle("merged")
        print(self.df)
        print(self.df["source"].values)
        # print(df["source"])
        # pass

    def store_pickle(self,df,num):
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-{num}.p", "wb" ))
    
    def load_pickle(self,num):
        return pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-{num}.p", "rb" ))

    def save_csv(self):
        self.df = self.df.sort_values("BBL")
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/merged.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        self.store_pickle(self.df,"merged")

if __name__ == '__main__':
    merge = Merge()

    # start = time.time()
    # merge.add_llid()
    # end = time.time()
    # print(end - start)

    merge.add_lbid()

    merge.save_csv()

    