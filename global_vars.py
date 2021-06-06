import requests
import docker
import time
import json
import pandas as pd
import re
import csv
import random
from selenium import webdriver
from bs4 import BeautifulSoup
import sys
import numpy as np
import difflib
import math
import pickle
import datetime
import psutil
import os
from sys import platform
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import uuid

global LOCAL_LOCUS_PATH
global LOCAL_WEBDRIVER_PATH

global manhattan_zips, non_manhattan_zips
global no_city, fuzzy_nyc, bronx, staten_island, brooklyn, new_york, queens

nyc_zips = ["10151","10026","10027","10030","10037","10039","10001","10011","10018","10019","10020","10036","10029","10035","10010","10016","10017","10022","10012","10013","10014","10004","10005","10006","10007","10038","10280","10002","10003","10009","10021","10028","10044","10065","10075","10128","10023","10024","10025","10031","10032","10033","10034","10040","10041","10043","10045","10055","10060","10069","10119","10103","10080","10081","10087","10090","10102","10103","10104","10105","10106","10107","10109","10110","10111","10112","10114","10115","10117","10118","10119","10120","10121","10122","10123","101124","10125","10126","10128","10130","10131","10132","10133","10138","10152","10153","10154","10155","10157","10158","10160","10162","10164","10165","10166","10167","10168","10169","10170","10171","10172","10173","10174","10175","10176","10177","10178","10179","10199","10199","10203","10211","10212","10213","10256","10258","10259","10260","10261","10265","10269","10270","10271","10273","10275","10277","10278","10279","10281","10282","10285","10286","10453","10457","10460","10458","10467","10468","10451","10452","10456","10454","10455","10459","10474","10463","10471","10466","10469","10470","10475","10461","10462","10464","10465","10472","10473","11212","11213","11216","11233","11238","11209","11214","11228","11204","11218","11219","11230","11234","11236","11239","11223","11224","11229","11235","11201","11205","11215","11217","11231","11203","11210","11225","11226","11207","11208","11211","11222","11220","11232","11206","11221","11237","11361","11362","11363","11364","11354","11355","11356","11357","11358","11359","11360","11365","11366","11367","11412","11423","11432","11433","11434","11435","11436","11101","11102","11103","11104","11105","11106","11374","11375","11379","11385","11691","11692","11693","11694","11695","11697","11004","11005","11411","11413","11422","11426","11427","11428","11429","11414","11415","11416","11417","11418","11419","11420","11421","11368","11369","11370","11372","11373","11377","11378","10302","10303","10310","10306","10307","10308","10309","10312","10301","10304","10305","10314","11241","11242","11243","11245","11249","11251","11252","11256","10311","11120","11351","11359","11371","11381","11405","11425","11430","11437","11439","11451","11499"]

not_nyc = ['EAST ROCKAWAY','BRONXVILLE']
fuzzy_nyc = ['OZONE PARK','HOLLIS','DOUGLASTON','BRIARWOOD','BELLE HARBOR','BRONX','STATEN ISLAND','BROOKLYN','BKLYN','INWOOD','NEW YORK','MANHATTAN','ROOSEVELT ISLAND','WARDS ISLAND','ARVERNE','QUEENS','ROCKAWAY PARK','ROCKAWAY POINT','ROCKAWAY BEACH','SUNNYSIDE','FLUSHING','BROAD CHANNEL','QUEENS VILLAGE','SOUTH OZONE PARK','RICHMOND HILL','SOUTH RICHMOND HILL','REGO PARK','RIDGEWOOD','ROSEDALE','ST ALBANS','SAINT ALBANS','WHITESTONE','HOLLISWOOD','WOODHAVEN','WOODSIDE','SPRINGFIELD GARDENS','LONG ISLAND CITY','LIC','L.I.C.','HOLLIS HILLS','HOWARD BEACH','JACKSON HEIGHTS','KEW GARDENS HILLS','CAMBRIA HEIGHTS','BELLEROSE','ASTORIA','BAYSIDE','BELLEROSE MANOR','BREEZY POINT','COLLEGE POINT','CORONA','EAST ELMHURST','ELMHURST','FAR ROCKAWAY','FLORAL PARK','FOREST HILLS','FRESH MEADOWS','GLENDALE','GLEN OAKS','JAMAICA','JAMAICA ESTATES','KEW GARDENS','LITTLE NECK','MASPETH','LAURELTON','MIDDLE VILLAGE','OAKLAND GARDENS']
bronx = ['BRONX']
staten_island = ['STATEN ISLAND']
brooklyn = ['BROOKLYN','BKLYN']
new_york = ['INWOOD','NEW YORK','MANHATTAN','ROOSEVELT ISLAND','WARDS ISLAND']
queens = ['OZONE PARK','HOLLIS','DOUGLASTON','BRIARWOOD','BELLE HARBOR','ARVERNE','QUEENS','ROCKAWAY PARK','ROCKAWAY POINT','ROCKAWAY BEACH','SUNNYSIDE','FLUSHING','BROAD CHANNEL','QUEENS VILLAGE','SOUTH OZONE PARK','RICHMOND HILL','SOUTH RICHMOND HILL','REGO PARK','RIDGEWOOD','ROSEDALE','ST ALBANS','SAINT ALBANS','WHITESTONE','HOLLISWOOD','WOODHAVEN','WOODSIDE','SPRINGFIELD GARDENS','LONG ISLAND CITY','LIC','L.I.C.','HOLLIS HILLS','HOWARD BEACH','JACKSON HEIGHTS','KEW GARDENS HILLS','CAMBRIA HEIGHTS','BELLEROSE','ASTORIA','BAYSIDE','BELLEROSE MANOR','BREEZY POINT','COLLEGE POINT','CORONA','EAST ELMHURST','ELMHURST','FAR ROCKAWAY','FLORAL PARK','FOREST HILLS','FRESH MEADOWS','GLENDALE','GLEN OAKS','JAMAICA','JAMAICA ESTATES','KEW GARDENS','LITTLE NECK','MASPETH','MIDDLE VILLAGE','LAURELTON','OAKLAND GARDENS']

if platform == "darwin":
    # MAC OS X
    LOCAL_LOCUS_PATH = os.path.expanduser("~") + "/Dropbox/locus/"
    LOCAL_WEBDRIVER_PATH = "/usr/local/bin/chromedriver"
elif platform == "linux":
	# LINUX
    LOCAL_LOCUS_PATH = "/home/ubuntu/locus/"
    LOCAL_WEBDRIVER_PATH = "/home/ubuntu/chromedriver"
else:
    # WINDOWS
    LOCAL_LOCUS_PATH = "C:/Users/jsbmm/Dropbox/locus/"
    LOCAL_WEBDRIVER_PATH = "C:/Program Files/chromedriver.exe"

global GLOBAL_COUNTER
GLOBAL_COUNTER = 0
global GLOBAL_LEN
GLOBAL_LEN = 0

def global_counter_init(curlen):
    global GLOBAL_COUNTER
    global GLOBAL_LEN
    GLOBAL_COUNTER = 0
    GLOBAL_LEN = curlen

def global_counter_tick():
    global GLOBAL_COUNTER
    global GLOBAL_LEN
    GLOBAL_COUNTER+=1
    if round(GLOBAL_COUNTER/GLOBAL_LEN,4)>round((GLOBAL_COUNTER-1)/GLOBAL_LEN,4):
        print(str(round(100*GLOBAL_COUNTER/GLOBAL_LEN,2)) + "%")

def get_result(inject):
    response = requests.get("https://api.cityofnewyork.us/geoclient/v1/search.json?input="+ inject +"&app_id=d4aa601d&app_key=f75348e4baa7754836afb55dc9b6363d")
    decoded = response.content.decode("utf-8")
    json_loaded = json.loads(decoded)
    return json_loaded['results'][0]['response']['bbl']

def add_bbl(row, overwrite=True):  # input row must have headers 'Building Number,' 'Street,' 'City,' 'Zip,' 'BBL'
    if overwrite or len(row["BBL"])==0:
        try:
            row["BBL"]=get_result(row['Building Number'] + " " + row['Street'] + " " + row['City'])
        except:
            try:
                row["BBL"]=get_result(row['Building Number'] + " " + row['Street'] + " " + row['Zip'])
            except:
                row["BBL"]=""

    global_counter_tick()
    return row

def lev_city(st, city_list):
    levs = []
    for i in city_list:
        levs.append(fuzz.ratio(st,i))
    return levs 

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

def clean_zip_city(df): #input df must have have header 'City' and 'Zip'
    
    df['State'] = df['State'].astype(str)    
    df['State'] = df['State'].str.upper()
    df['State'] = df['State'].replace(['N/A','NULL','nan'],'')
    df['State'] = df['State'].fillna("")
    
    df["Zip"] = df["Zip"].astype(str)
    df['Zip'] = df['Zip'].replace(['N/A','NULL','nan'],'')
    df["Zip"] = df["Zip"].fillna("")
    df["Zip"] = df["Zip"].apply(lambda x: x.strip(' '))
    df["Zip"] = df["Zip"].apply(lambda x: str(int(float(x))) if x!='' and str(x).isdigit and len(str(int(float(x))))==5 else '')
    
    df["City"] = df["City"].astype(str)
    df['City'] = df['City'].str.upper()
    df['City'] = df['City'].replace(['N/A','NULL','nan'],'')
    df["City"] = df["City"].fillna("")

    indices = []

    for index, row in df.iterrows(): 
    
        if (max(lev_city(row['City'], not_nyc)) > 90) | ((row['City'] =='') & (row['Zip']=='')):
            pass

        elif (max(lev_city(row['City'], fuzzy_nyc)) > 85):
            df.at[index, 'City'] = boro_assign(fuzzy_nyc[lev_city(row['City'], fuzzy_nyc).index(max(lev_city(row['City'], fuzzy_nyc)))])
            
            if row['Zip'] not in nyc_zips:
                df.at[index, 'Zip'] = ''
            
            if (row['State']!='NY'):
                df.at[index, 'State'] = ''
                
            indices.append(index)

        elif (row['Zip'] in nyc_zips):
            df.at[index, 'City'] = ''

            if (row['State']!='NY'):
                df.at[index, 'State'] = ''

            indices.append(index)

        else:
            pass
    
        global_counter_tick()

    df = df.loc[indices]
    df = df.reset_index(drop=True)
    
    return df 