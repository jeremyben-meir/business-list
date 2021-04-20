#######IMPORTS#######

from global_vars import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import uuid

#######FUNCTION DEFINITIONS#########


########
# compare industry -> lower fuzzy match bar?
# mutually exclusive industries

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


def reorder(df):
    # df = df[["LBID","LLID","Business Name","Record ID","Contact Phone","BBL","Building Number","Street","Street 2","Unit","Unit Type","Description","City","State","Zip","Industry","License Type","Active Vehicles","Moved To","Moved From"]]
    df = df[['Record ID', 'Business Name', 'INSP Date', 'Industry', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit','Description', 'City', 'State', 'Zip', 'BBL', 'Application ID','License Type', 'Application or Renewal', 'APP Status','APP Status Date', 'APP Start Date', 'APP End Date','License Start Date', 'License Expiration Date','Temp Op Letter Issued', 'Temp Op Letter Expiration', 'Contact Phone','CHRG Date']]
    return df

def nat_check(nat):
    # return nat == np.datetime64('NaT') or pd.isnull(nat)
    return pd.isnull(nat)

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

def sub(m):
    words_ignore = set(["corp", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon', 'smoke','pizza','of','the'])
    return '' if m.group() in words_ignore else m.group()

def namelists_to_match(names_list, names_list_2):
    max_ratio = 0
    if len(names_list)>0 and len(names_list_2)>0:
        for x in names_list:
            for y in names_list_2:
                fuzz_ratio = fuzz.WRatio(re.sub(r'\w+', sub, x.lower()),re.sub(r'\w+', sub, y.lower()))
                max_ratio = (fuzz_ratio if fuzz_ratio > max_ratio else max_ratio)
    return max_ratio

def phonelists_to_match(names_list, names_list_2):
    if len(names_list)>0 and len(names_list_2)>0:
        for x in names_list:
            for y in names_list_2:
                if x == y:
                    return True
    return False

def names_to_match(name0, name1):
    return fuzz.WRatio(re.sub(r'\w+', sub, name0.lower()),re.sub(r'\w+', sub, name1.lower()))

def phones_to_match(phone0, phone1):
    return phone0 == phone1 and len(phone0)>6

def isNaN(string):
    return string != string

def type_cast(df):
    # df['Record ID'] = df['Record ID'].astype(str)
    # df['Business Name'] = df['Business Name'].astype(str)
    df['APP Start Date'] = df['APP Start Date'].astype('datetime64[D]')
    df['APP Status Date'] = df['APP Status Date'].astype('datetime64[D]')
    df['APP End Date'] = df['APP End Date'].astype('datetime64[D]')
    df['License Start Date'] = df['License Start Date'].astype('datetime64[D]')
    df['License Expiration Date'] = df['License Expiration Date'].astype('datetime64[D]')
    df['Temp Op Letter Issued'] = df['Temp Op Letter Issued'].astype('datetime64[D]')
    df['Temp Op Letter Expiration'] = df['Temp Op Letter Expiration'].astype('datetime64[D]')
    df['CHRG Date'] = df['CHRG Date'].astype('datetime64[D]')

    return df

def find_sets(index0, row0, group, indexlist):
    indexlist.append(index0)
    for index1,row1 in group.iterrows():
        if index1 not in indexlist:
            if row0["Record ID"] == row1["Record ID"] or phones_to_match(row0["Contact Phone"],row1["Contact Phone"]) or names_to_match(row0["Business Name"],row1["Business Name"]) > 80 :
                indexlist = find_sets(index1,row1,group,indexlist)
    return indexlist
        

def geo_combine(df, bbl):
    rowlist = []
    mask = (df['BBL'].str.len() == 10) & (df['BBL']!="0000000000")
    curgrp = df[mask].groupby('BBL') if bbl else df.groupby(['Building Number','Street','City'])

    thislen = len(curgrp) ##PRINT
    counter = 0 ##PRINT
    for name , group in curgrp:
        counter += 1 ##PRINT
        print(float(counter)/thislen) ##PRINT
        excludelist = []
        
        for index0,row0 in group.iterrows():
            indexlist = []
            if index0 not in excludelist:
                indexlist = find_sets(index0, row0, group, indexlist)
                excludelist += indexlist
                group.loc[indexlist]["LLID"] = "hey"
                print(group.loc[indexlist])
                # row = group.loc[indexlist].groupby('BBL',as_index=False).agg(lambda x: list(x))
                # rowlist+= row.values.tolist()

    return pd.DataFrame(rowlist, columns=df.columns)


def load_source_files():

    df_o1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "rb" ))
    df_o2 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "rb" ))
    df_o3 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "rb" ))

    df = pd.concat([df_o1,df_o2,df_o3], axis=0, join='outer', ignore_index=False)

    df = type_cast(df)

    totlen = len(df)
    df = df.replace(nyc_city,correct_nyc_city)
    df = df.apply(lambda row: add_bbl(row, totlen=totlen), axis=1)

    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-1.p", "wb" ))
    return df

def process_0(df_1):

    df_1["LLID"] = ""

    df_1 = geo_combine(df_1, bbl=True)

    pickle.dump(df_1, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-5.p", "wb" ))
    return df_1


def process_1(df_1):

    df_1 = geo_combine(df_1, bbl=False)

    pickle.dump(df_1, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-2.p", "wb" ))

    return df_1
    
def process_2(df_2):

    df_2["FIRST"] = df_2[['INSP Date','APP Status Date','APP Start Date','APP End Date','License Start Date','License Expiration Date','Temp Op Letter Issued','Temp Op Letter Expiration','CHRG Date']].min(axis=1)
    df_2 = df_2.sort_values(by='FIRST', ascending=False)
    del df_2["FIRST"]

    df_2['LLID'] = [str(uuid.uuid4()) for _ in range(len(df_2.index))]
    df_2['LLID'] = df_2['LLID'].astype(str)

    pickle.dump(df_2, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-3.p", "wb" ))
    return df_2

def process_3(df_3):

    # df_2 = df_2.groupby(['Record ID']).agg(lambda x: list(x)).reset_index() DO INSTEAD WITH LISTS

    df_3['Moved From'] = np.empty((len(df_3), 0)).tolist()
    df_3['Moved To'] = np.empty((len(df_3), 0)).tolist()
    df_3['Moved From'] = df_3['LLID'].apply(lambda x: x[1:] + [np.nan])
    df_3['Moved To'] = df_3['LLID'].apply(lambda x: [np.nan]+x[:-1])
    df_3['LBID'] = [str(uuid.uuid4()) for _ in range(len(df_3.index))]
    df_3['LBID'] = df_3['LBID'].astype(str)

    df_3 = explode(df_3, ['Business Name','INSP Date','Industry','Building Number','Street','Street 2','Unit Type','Unit','Description','City','State','Zip','BBL','Application ID','License Type','Application or Renewal','APP Status','APP Status Date','APP Start Date','APP End Date','License Start Date','License Expiration Date','Temp Op Letter Issued','Temp Op Letter Expiration','Contact Phone','CHRG Date','LLID','Moved From','Moved To'], fill_value='')#

    pickle.dump(df_3, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-4.p", "wb" ))
    return df_3

def process_4(df_4):

    # df_4 = reorder(df_4)
    pickle.dump(df_4, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-5.p", "wb" ))
    return df_4

def begin_process():
    # df_1 = load_source_files()
    # print("load done")
    df_1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-1.p", "rb" ))
    df_2 = process_0(df_1)
    print("process 0 done")
    # df_2 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-2.p", "rb" ))
    # df_3 = process_1(df_2)
    # print("process 1 done")
    # df_3 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-3.p", "rb" ))
    # df_4 = process_2(df_3)
    # print("process 2 done")
    # df_4 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-4.p", "rb" ))
    # df_5 = process_3(df_4)
    # print("process 3 done")
    # df_5 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-5.p", "rb" ))

    # df_5 = geo_combine(df_5, setting=False)

    # pickle.dump(df_5, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-6.p", "wb" ))

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/inspection_application.csv"
    df_2.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)


if __name__ == '__main__':
    begin_process()