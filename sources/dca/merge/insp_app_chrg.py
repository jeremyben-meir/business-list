#######IMPORTS#######

from global_vars import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import uuid

#######FUNCTION DEFINITIONS#########


def reorder(df):
    df = df[["LBID","LLID","Business Name","Record ID","Contact Phone","BBL","BBL Latest","Building Number","Street","Street 2","Unit","Unit Type","Description","City","State","Zip","Industry","License Type","Active Vehicles","Moved To","Moved From","First Temp Op Letter Issued","Last Temp Op Letter Issued","Last Temp Op Letter Expiration","Date of First Application","Date of Last Application","Date of First Close","Date of Last Close","Date of First OOB","Date of Last OOB","Date of First Inspection","Date of Last Inspection","Date of First Charge","Date of Last Charge"]]
    return df

def nat_check(nat):
    return nat == np.datetime64('NaT') or pd.isnull(nat)

def min_time(time1,time2):
    if nat_check(time1) and nat_check(time2):
        return np.datetime64('NaT')
    elif nat_check(time1):
        return time2
    elif nat_check(time2):
        return time1
    else:
        return min(time1,time2)

def max_time(time1,time2):
    if nat_check(time1) and nat_check(time2):
        return np.datetime64('NaT')
    elif nat_check(time1):
        return time2
    elif nat_check(time2):
        return time1
    else:
        return max(time1,time2)

def progress_meter(meter, limit, prior):
    length = 1000.0
    progress = int((float(meter)/float(limit)) * length)
    global timestart
    global timecount
    if meter == 0:

        print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        print('Time remaining: calculating...')
        print("Writing CSV" + ": [" + "-"*int(length) + "]")

        timecount = 0.0
        timestart = time.time()
    elif progress - prior > 0:
        print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        print("CPU Usage: " + str(psutil.cpu_percent()))
        print("Memory Usage: " + str(psutil.virtual_memory().percent))

        timecount+=1.0
        x= datetime.datetime.now()
        seconds_away = int(float(time.time() - timestart)*(float(length)/float(timecount)) - float(time.time() - timestart))
        y = x + datetime.timedelta(0,seconds_away)

        print('Time remaining: ' + str(seconds_away)+ " seconds (" + str(y.strftime("%I:%M:%S%p")) + ")")
        print("Writing CSV" + ": [" + "#"*int(timecount) + "-"*int(length-timecount) + "] "+ str(int(progress/10.0))+"%")

    return meter + 1  , progress

def sub(m):
    words_ignore = set(["corp", "//$L$//", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon', 'smoke','pizza','of','the'])
    return '' if m.group() in words_ignore else m.group()

def names_to_match(names_list, names_list_2):
    max_ratio = 0
    if len(names_list)>0 and len(names_list_2)>0:
        for x in names_list:
            for y in names_list_2:
                fuzz_ratio = fuzz.WRatio(re.sub(r'\w+', sub, x.lower()),re.sub(r'\w+', sub, y.lower()))
                max_ratio = (fuzz_ratio if fuzz_ratio > max_ratio else max_ratio)
    return max_ratio

def phones_to_match(names_list, names_list_2):
    if len(names_list)>0 and len(names_list_2)>0:
        for x in names_list:
            for y in names_list_2:
                if x == y:
                    return True
    return False

def get_unique_and_flatten(my_list):
    result_list = []
    for x in my_list:
        result_list += x.split("//$L$//")
    result_list = [i for i in result_list if i] 
    return list(set(result_list))

def type_cast(df):
    df["Record ID"] = df["Record ID"].astype(str).fillna("").replace("nan","")
    df["License Type"] = df["License Type"].astype(str).fillna("").replace("nan","")
    df["Business Name"] = df["Business Name"].astype(str).fillna("").replace("nan","")
    df["Industry"] = df["Industry"].astype(str).fillna("").replace("nan","")
    df["Building Number"] = df["Building Number"].astype(str).fillna("").replace("nan","")
    df["Street"] = df["Street"].astype(str).fillna("").replace("nan","")
    df["Street 2"] = df["Street 2"].astype(str).fillna("").replace("nan","")
    df["Unit Type"] = df["Unit Type"].astype(str).fillna("").replace("nan","")
    df["Unit"] = df["Unit"].astype(str).fillna("").replace("nan","")
    df["Description"] = df["Description"].astype(str).fillna("").replace("nan","")
    df["City"] = df["City"].astype(str).fillna("").replace("nan","")
    df["State"] = df["State"].astype(str).fillna("").replace("nan","")
    df["Zip"] = df["Zip"].apply(lambda x: str(int(float(x))) if not math.isnan(x) else "").astype(str).fillna("").replace("nan","")
    df["Contact Phone"] = df["Contact Phone"].astype(str).replace("EXT","").fillna("").replace("nan","").replace("-","")
    df["Contact Phone"] = df["Contact Phone"].apply(lambda x: str(int(float(x))) if x != "" and x.isdigit() and not math.isnan(float(x)) else "").astype(str)
    df["Active Vehicles"] = df["Active Vehicles"].astype(str).fillna("").replace("nan","")
    df["BBL"] = df["BBL"].apply(lambda x: str(int(float(x))) if not math.isnan(x) else "").astype(str).fillna("").replace("nan","")
    df["BBL"] = df["BBL"].apply(lambda x: "" if x != "" and int(float(x)) == 0 else x).astype(str).fillna("").replace("nan","")
    df["BBL Latest"] = df["BBL Latest"].apply(lambda x: str(int(float(x))) if not math.isnan(x) else "").astype(str).fillna("").replace("nan","")
    df["BBL Latest"] = df["BBL Latest"].apply(lambda x: "" if x != "" and int(float(x)) == 0 else x).astype(str).fillna("").replace("nan","")
    df["First Temp Op Letter Issued"] = df["Temp Op Letter Issued"].astype("datetime64[D]")
    df["Last Temp Op Letter Issued"] = df["Temp Op Letter Issued"].astype("datetime64[D]")
    df["Last Temp Op Letter Expiration"] = df["Temp Op Letter Expiration"].astype("datetime64[D]")
    df["Date of First Application"] = df["Date of First Application"].astype("datetime64[D]")
    df["Date of Last Application"] = df["Date of Last Application"].astype("datetime64[D]")
    df["Date of First Close"] = df["Date of First Close"].astype("datetime64[D]")
    df["Date of Last Close"] = df["Date of Last Close"].astype("datetime64[D]")
    df["Date of First OOB"] = df["Date of First OOB"].astype("datetime64[D]")
    df["Date of Last OOB"] = df["Date of Last OOB"].astype("datetime64[D]")
    df["Date of First Inspection"] = df["Date of First Inspection"].astype("datetime64[D]")
    df["Date of Last Inspection"] = df["Date of Last Inspection"].astype("datetime64[D]")
    df["Date of First Charge"] = df["Date of First Charge"].astype("datetime64[D]")
    df["Date of Last Charge"] = df["Date of Last Charge"].astype("datetime64[D]")
    return df

def isNaN(string):
    return string != string

def flatten_and_elim_nan(row):
    my_row = row
    to_flatten = ['License Type','Business Name','First Temp Op Letter Issued','Last Temp Op Letter Issued', 'Last Temp Op Letter Expiration', 'Industry', 'Building Number','Street','Street 2', 'Unit Type','Unit','Description', 'City', 'State', 'Zip', 'Contact Phone','Active Vehicles','BBL','BBL Latest','Date of First Application','Date of Last Application','Date of First Close','Date of Last Close','Date of First OOB','Date of Last OOB','Date of First Inspection','Date of Last Inspection','Date of First Charge','Date of Last Charge','LLID']
    
    row_dim = len(my_row['Building Number'])
    
    x = 0
    while x < row_dim:
        if ((len(my_row['BBL'][x]) < 4 or isNaN(my_row['BBL'][x])) and ((len(my_row['BBL Latest'][x]) < 4) or isNaN(my_row['BBL Latest'][x]))):
            if x>0:
                my_row['Date of First Application'][x-1]=min_time(my_row['Date of First Application'][x],my_row['Date of First Application'][x-1])
                my_row['Date of Last Application'][x-1]=max_time(my_row['Date of Last Application'][x],my_row['Date of Last Application'][x-1])
                my_row['Date of First Inspection'][x-1]=min_time(my_row['Date of First Inspection'][x],my_row['Date of First Inspection'][x-1])
                my_row['Date of Last Inspection'][x-1]=max_time(my_row['Date of Last Inspection'][x],my_row['Date of Last Inspection'][x-1])
                my_row['Date of First Close'][x-1]=min_time(my_row['Date of First Close'][x],my_row['Date of First Close'][x-1])
                my_row['Date of Last Close'][x-1]=max_time(my_row['Date of Last Close'][x],my_row['Date of Last Close'][x-1])
                my_row['Date of First OOB'][x-1]=min_time(my_row['Date of First OOB'][x],my_row['Date of First OOB'][x-1])
                my_row['Date of Last OOB'][x-1]=max_time(my_row['Date of Last OOB'][x],my_row['Date of Last OOB'][x-1])
                my_row['Date of First Charge'][x-1]=min_time(my_row['Date of First Charge'][x],my_row['Date of First Charge'][x-1])
                my_row['Date of Last Charge'][x-1]=max_time(my_row['Date of Last Charge'][x],my_row['Date of Last Charge'][x-1])
                for y in to_flatten:
                    del my_row[y][x]
            elif row_dim > 1:
                my_row['Date of First Application'][x+1]=min_time(my_row['Date of First Application'][x],my_row['Date of First Application'][x+1])
                my_row['Date of Last Application'][x+1]=max_time(my_row['Date of Last Application'][x],my_row['Date of Last Application'][x+1])
                my_row['Date of First Inspection'][x+1]=min_time(my_row['Date of First Inspection'][x],my_row['Date of First Inspection'][x+1])
                my_row['Date of Last Inspection'][x+1]=max_time(my_row['Date of Last Inspection'][x],my_row['Date of Last Inspection'][x+1])
                my_row['Date of First Close'][x+1]=min_time(my_row['Date of First Close'][x],my_row['Date of First Close'][x+1])
                my_row['Date of Last Close'][x+1]=max_time(my_row['Date of Last Close'][x],my_row['Date of Last Close'][x+1])
                my_row['Date of First OOB'][x+1]=min_time(my_row['Date of First OOB'][x],my_row['Date of First OOB'][x+1])
                my_row['Date of Last OOB'][x+1]=max_time(my_row['Date of Last OOB'][x],my_row['Date of Last OOB'][x+1])
                my_row['Date of First Charge'][x+1]=min_time(my_row['Date of First Charge'][x],my_row['Date of First Charge'][x+1])
                my_row['Date of Last Charge'][x+1]=max_time(my_row['Date of Last Charge'][x],my_row['Date of Last Charge'][x+1])
                for y in to_flatten:
                    del my_row[y][x]
            row_dim-=1
        else:
            x+=1
    return my_row

def geo_combine(df, setting):
    main_iterator = df[(df["BBL Latest"]=="") & (df["BBL"] == "")]
    main_iterator = main_iterator[main_iterator.apply(lambda df_row: len(df_row["Building Number"])>df_row["Building Number"].count('') and len(df_row["Street"])>df_row["Street"].count(''), axis= 1)]
    iterator = df if setting else main_iterator

    progress = Progress_meter(limit = len(iterator.index),length=1000)
    indexes_dropped = []

    for index, row in iterator.iterrows():

        progress = progress.display()
        # if progress.meter>20:
        #     print('fuck it im out')
        #     break

        if index not in indexes_dropped:
            group = df.loc[((df["BBL"] == row["BBL"]) & (df["BBL"] != "")) | ((df["BBL Latest"] == row["BBL Latest"]) & (df["BBL Latest"]!="")) ] if setting else iterator[iterator.apply(lambda df_row: df_row["Building Number"][0]==row["Building Number"][0] and df_row["Street"][0] == row["Street"][0], axis= 1)]

            for index1, row1 in group.iterrows():
                if index != index1 and (phones_to_match(row["Contact Phone"],row1["Contact Phone"]) or names_to_match(row["Business Name"],row1["Business Name"]) > 90):
                    for colname in ["Business Name","Contact Phone","Record ID","Industry","Building Number","Street","Street 2","Unit Type","Unit","Description","City","State","Zip","Active Vehicles"]:
                        df.at[index, colname] = df.loc[index, colname] + row1[colname]

                    df.at[index, "First Temp Op Letter Issued"] = min_time(df.loc[index, "First Temp Op Letter Issued"], row1["First Temp Op Letter Issued"])
                    df.at[index, "Last Temp Op Letter Issued"] = max_time(df.loc[index, "Last Temp Op Letter Issued"], row1["Last Temp Op Letter Issued"])
                    df.at[index, "Last Temp Op Letter Expiration"] = max_time(df.loc[index, "Last Temp Op Letter Expiration"], row1["Last Temp Op Letter Expiration"])
                    df.at[index, "Date of First Application"] = min_time(df.loc[index, "Date of First Application"], row1["Date of First Application"])
                    df.at[index, "Date of Last Application"] = max_time(df.loc[index, "Date of Last Application"], row1["Date of Last Application"])
                    df.at[index, "Date of First Close"] = min_time(df.loc[index, "Date of First Close"], row1["Date of First Close"])
                    df.at[index, "Date of Last Close"] = max_time(df.loc[index, "Date of Last Close"], row1["Date of Last Close"])
                    df.at[index, "Date of First OOB"] = min_time(df.loc[index, "Date of First OOB"], row1["Date of First OOB"])
                    df.at[index, "Date of Last OOB"] = max_time(df.loc[index, "Date of Last OOB"], row1["Date of Last OOB"])
                    df.at[index, "Date of First Inspection"] = min_time(df.loc[index, "Date of First Inspection"], row1["Date of First Inspection"])
                    df.at[index, "Date of Last Inspection"] = max_time(df.loc[index, "Date of Last Inspection"], row1["Date of Last Inspection"])
                    df.at[index, "Date of First Charge"] = min_time(df.loc[index, "Date of First Charge"], row1["Date of First Charge"])
                    df.at[index, "Date of Last Charge"] = max_time(df.loc[index, "Date of Last Charge"], row1["Date of Last Charge"])

                    df["LBID"]=df["LBID"].replace(row1["LBID"],row["LBID"])
                    df[["LLID","Moved To","Moved From"]]=df[["LLID","Moved To","Moved From"]].replace(row1["LLID"],row["LLID"])

                    if row1["Moved To"] != "nan" and row1["Moved To"] != "" and not pd.isna(row1["Moved To"]) and not pd.isnull(row1["Moved To"]):
                        if row["Moved To"] == "nan" or row["Moved To"] == "" or pd.isna("Moved To") or pd.isnull("Moved To"):
                            df.at[index, "Moved To"] = row1["Moved To"]
                        else:
                            df[["LLID","Moved To","Moved From"]]=df[["LLID","Moved To","Moved From"]].replace(row1["Moved To"],row["Moved To"])

                    if row1["Moved From"] != "nan" and row1["Moved From"] != "" and not pd.isna(row1["Moved From"]) and not pd.isnull(row1["Moved From"]):
                        if row["Moved From"] == "nan" or row["Moved From"] == "" or pd.isna("Moved From") or pd.isnull("Moved From"):
                            df.at[index, "Moved From"] = row1["Moved From"]
                        else:
                            df[["LLID","Moved To","Moved From"]]=df[["LLID","Moved To","Moved From"]].replace(row1["Moved From"],row["Moved From"])

                    
                    df=df.drop(index1)
                    iterator=iterator.drop(index1)
                    indexes_dropped.append(index1)
    # if setting:
    return df
    # return iterator


def load_source_files():

    application_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/applications_updated_0.csv"
    inspection_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/inspections_updated_0.csv"
    charge_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/charges_updated_0.csv"
    df_o1 = pd.read_csv(application_file_path)
    df_o2 = pd.read_csv(inspection_file_path)
    df_o3 = pd.read_csv(charge_file_path)

    df_0 = pd.concat([df_o1,df_o2,df_o3], axis=0, join='outer', ignore_index=False)

    df_0 = type_cast(df_0)

    pickle.dump(df_0, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-1.p", "wb" ))
    return df_0

def process_0():
    df_1 = load_source_files()

    print(df_1.dtypes)


    df_1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-1.p", "rb" ))
    group_0 = df_1.groupby(['Record ID','Building Number', 'Street']).agg({'License Type': 'max','Business Name':"//$L$//".join,'First Temp Op Letter Issued': 'min','Last Temp Op Letter Issued': 'max','Last Temp Op Letter Expiration': 'max', 'Industry': lambda x: x.iloc[0], 'Street 2':lambda x: x.iloc[0], 'Unit Type':lambda x: x.iloc[0],'Unit':lambda x: x.iloc[0],'Description':lambda x: x.iloc[0], 'City':lambda x: x.iloc[0], 'State':lambda x: x.iloc[0], 'Zip':lambda x: x.iloc[0], 'Contact Phone':"//$L$//".join,'Active Vehicles':"//$L$//".join,'BBL':'max','BBL Latest':'max','Date of First Application': 'min','Date of Last Application':'max','Date of First Close': 'min','Date of Last Close':'max','Date of First OOB': 'min','Date of Last OOB':'max','Date of First Inspection': 'min','Date of Last Inspection':'max','Date of First Charge': 'min','Date of Last Charge':'max'})#
    df_1 = pd.DataFrame(group_0).reset_index()
    
    df_temp_0 = df_1[df_1["BBL Latest"]==""]###########
    df_temp_1 = df_1[df_1["BBL Latest"]!=""]

    group_0 = df_temp_1.groupby(['Record ID','BBL Latest']).agg({'License Type': 'max','Business Name':"//$L$//".join,'First Temp Op Letter Issued': 'min','Last Temp Op Letter Issued': 'max','Last Temp Op Letter Expiration': 'max', 'Industry': "//$L$//".join, 'Building Number': "//$L$//".join,'Street':"//$L$//".join,'Street 2':"//$L$//".join, 'Unit Type':"//$L$//".join,'Unit':"//$L$//".join,'Description':"//$L$//".join, 'City':"//$L$//".join, 'State':"//$L$//".join, 'Zip':"//$L$//".join, 'Contact Phone':"//$L$//".join,'Active Vehicles':"//$L$//".join,'BBL':'max','Date of First Application': 'min','Date of Last Application':'max','Date of First Close': 'min','Date of Last Close':'max','Date of First OOB': 'min','Date of Last OOB':'max','Date of First Inspection': 'min','Date of Last Inspection':'max','Date of First Charge': 'min','Date of Last Charge':'max'})#
    df_temp_1 = pd.DataFrame(group_0).reset_index()

    df_temp_2 = df_temp_1[df_temp_1["BBL"]==""]##########
    df_temp_3 = df_temp_1[df_temp_1["BBL"]!=""]

    group_0 = df_temp_3.groupby(['Record ID','BBL']).agg({'License Type': 'max','Business Name':"//$L$//".join, 'Industry': "//$L$//".join, 'Building Number': "//$L$//".join,'Street':"//$L$//".join,'Street 2':"//$L$//".join, 'Unit Type':"//$L$//".join,'Unit':"//$L$//".join,'Description':"//$L$//".join, 'City':"//$L$//".join, 'State':"//$L$//".join, 'Zip':"//$L$//".join, 'Contact Phone':"//$L$//".join,'Active Vehicles':"//$L$//".join,'BBL Latest':'max','First Temp Op Letter Issued': 'min','Last Temp Op Letter Issued': 'max','Last Temp Op Letter Expiration': 'max','Date of First Application': 'min','Date of Last Application':'max','Date of First Close': 'min','Date of Last Close':'max','Date of First OOB': 'min','Date of Last OOB':'max','Date of First Inspection': 'min','Date of Last Inspection':'max','Date of First Charge': 'min','Date of Last Charge':'max'})#
    df_temp_3 = pd.DataFrame(group_0).reset_index()

    df_1 = pd.concat([df_temp_0,df_temp_2,df_temp_3], axis=0, join='outer', ignore_index=False)

 
    pickle.dump(df_1, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-2.p", "wb" ))

    return df_1
    
def process_1():
    df_2 = process_0()
    df_2 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-2.p", "rb" ))

    df_2["FIRST"] = df_2[['First Temp Op Letter Issued', 'Date of First Application', 'Date of First Inspection', 'Date of First Charge']].min(axis=1)

    df_2 = df_2.sort_values(by='FIRST', ascending=False)
    df_2['LLID'] = [str(uuid.uuid4()) for _ in range(len(df_2.index))]
    df_2['LLID'] = df_2['LLID'].astype(str)

    group_0 = df_2.groupby(['Record ID']).agg({'License Type':list,'Business Name':list, "LLID":list, 'Industry':list, 'Building Number':list,'Street':list,'Street 2':list, 'Unit Type':list,'Unit':list,'Description':list, 'City':list, 'State':list, 'Zip':list, 'Contact Phone':list,'Active Vehicles':list,'BBL':list,'BBL Latest':list,'First Temp Op Letter Issued':list,'Last Temp Op Letter Issued':list,'Last Temp Op Letter Expiration':list,'Date of First Application':list,'Date of Last Application':list,'Date of First Close':list,'Date of Last Close':list,'Date of First OOB':list,'Date of Last OOB':list,'Date of First Inspection':list,'Date of Last Inspection':list, 'Date of First Charge': list,'Date of Last Charge':list})#
    df_2 = pd.DataFrame(group_0).reset_index()

    pickle.dump(df_2, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-3.p", "wb" ))
    return df_2

def process_2():
    df_3 = process_1()
    df_3 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-3.p", "rb" ))

    df_3 = df_3.apply(lambda row: flatten_and_elim_nan(row), axis=1)

    df_3['Moved From'] = np.empty((len(df_3), 0)).tolist()
    df_3['Moved To'] = np.empty((len(df_3), 0)).tolist()
    df_3['Moved From'] = df_3['LLID'].apply(lambda x: x[1:] + [np.nan])
    df_3['Moved To'] = df_3['LLID'].apply(lambda x: [np.nan]+x[:-1])
    df_3['LBID'] = [str(uuid.uuid4()) for _ in range(len(df_3.index))]
    df_3['LBID'] = df_3['LBID'].astype(str)

    df_3 = explode(df_3, ['License Type','First Temp Op Letter Issued','Last Temp Op Letter Issued','Last Temp Op Letter Expiration', 'Industry', 'Building Number','Street','Street 2', 'Unit Type','Unit','Description', 'City', 'State', 'Zip','Active Vehicles','BBL','BBL Latest','Date of First Application','Date of Last Application','Date of First Close','Date of Last Close','Date of First OOB','Date of Last OOB','Date of First Inspection','Date of Last Inspection','Date of First Charge','Date of Last Charge','LLID','Moved From','Moved To'], fill_value='')#

    pickle.dump(df_3, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-4.p", "wb" ))
    return df_3

def process_3():
    # df_4 = process_2()

    df_4 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-4.p", "rb" ))

    # df_4= pd.read_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/gamestop.csv")
    # cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/gs1.csv"
    # df_4.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    df_4["Business Name"] = df_4["Business Name"].apply(lambda x: get_unique_and_flatten(x))
    df_4["Contact Phone"] = df_4["Contact Phone"].apply(lambda x: get_unique_and_flatten(x))

    df_4["Industry"] = df_4["Industry"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Building Number"] = df_4["Building Number"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Street"] = df_4["Street"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Street 2"] = df_4["Street 2"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Unit Type"] = df_4["Unit Type"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Unit"] = df_4["Unit"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Description"] = df_4["Description"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["City"] = df_4["City"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["State"] = df_4["State"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Zip"] = df_4["Zip"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4["Active Vehicles"] = df_4["Active Vehicles"].astype(str).apply(lambda x: x.split("//$L$//"))
    df_4['Moved To'] = df_4['Moved To'].astype(str)
    df_4['Moved From'] = df_4['Moved From'].astype(str)
    df_4['LBID'] = df_4['LBID'].astype(str)
    df_4['LLID'] = df_4['LLID'].astype(str)

    df_4["Record ID"] = df_4["Record ID"] .apply(lambda x: [x])

    df_4 = geo_combine(df_4, setting=True)

    # cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/gs2.csv"
    # df_4.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    df_4["First Temp Op Letter Issued"] = pd.to_datetime(df_4["First Temp Op Letter Issued"].astype("datetime64[D]")).dt.date
    df_4["Last Temp Op Letter Issued"] = pd.to_datetime(df_4["Last Temp Op Letter Issued"].astype("datetime64[D]")).dt.date
    df_4["Last Temp Op Letter Expiration"] = pd.to_datetime(df_4["Last Temp Op Letter Expiration"].astype("datetime64[D]")).dt.date
    df_4["Date of First Application"] = pd.to_datetime(df_4["Date of First Application"].astype("datetime64[D]")).dt.date
    df_4["Date of Last Application"] = pd.to_datetime(df_4["Date of Last Application"].astype("datetime64[D]")).dt.date
    df_4["Date of First Close"] = pd.to_datetime(df_4["Date of First Close"].astype("datetime64[D]")).dt.date
    df_4["Date of Last Close"] = pd.to_datetime(df_4["Date of Last Close"].astype("datetime64[D]")).dt.date
    df_4["Date of First OOB"] = pd.to_datetime(df_4["Date of First OOB"].astype("datetime64[D]")).dt.date
    df_4["Date of Last OOB"] = pd.to_datetime(df_4["Date of Last OOB"].astype("datetime64[D]")).dt.date
    df_4["Date of First Inspection"] = pd.to_datetime(df_4["Date of First Inspection"].astype("datetime64[D]")).dt.date
    df_4["Date of Last Inspection"] = pd.to_datetime(df_4["Date of Last Inspection"].astype("datetime64[D]")).dt.date
    df_4["Date of First Charge"] = pd.to_datetime(df_4["Date of First Charge"].astype("datetime64[D]")).dt.date
    df_4["Date of Last Charge"] = pd.to_datetime(df_4["Date of Last Charge"].astype("datetime64[D]")).dt.date

    df_4 = reorder(df_4)
    pickle.dump(df_4, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-5.p", "wb" ))
    return df_4

def begin_process():
    # df_5 = process_3()
    df_5 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-5.p", "rb" ))

    # df_5[df_5['LLID'].isin(df_5[df_5.duplicated(subset=['LLID'])]['LLID'].tolist())].to_csv("berding.csv",index=False)

    df_5 = geo_combine(df_5, setting=False)

    pickle.dump(df_5, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-6.p", "wb" ))

    # cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/gs2.csv"
    # df_5.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/inspection_application.csv"
    df_5.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)


if __name__ == '__main__':
    begin_process()