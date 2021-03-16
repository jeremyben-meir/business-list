from global_vars import *

import uuid
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def can_be_float(flt):
    if flt == "":
        return False
    try:
        float(flt)
        return True
    except:
        return False

def fix_bbl(row):

    try:
        row['Address City'] = correct_nyc_city[nyc_city.index(row['Address City'])] if row['Address City'] != "" else ""
    except:
        row['Address City'] = ""

    if (row["BBL Latest"]=="" or row["BBL Latest"]=="nan" or pd.isna(row["BBL Latest"]) or pd.isnull(row["BBL Latest"])):
        row["BBL Latest"]=row["BBL"]

    return row


def isblank(input_na):
    return (input_na=="") | (input_na == "nan") | (pd.isna(input_na)) | (pd.isnull(input_na))

def sub(m):
    words_ignore = set(["corp", "//$L$//", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon', 'smoke','pizza','of','the'])
    return '' if m.group() in words_ignore else m.group()

def names_to_match(names_list, names_list_2):
    max_ratio = 0
    if len(names_list)>0 and len(names_list_2)>0:
        for x in names_list:
            for y in names_list_2:
                if x != "" and y != "":
                    fuzz_ratio = fuzz.WRatio(re.sub(r'\w+', sub, x.lower()),re.sub(r'\w+', sub, y.lower()))
                    max_ratio = (fuzz_ratio if fuzz_ratio > max_ratio else max_ratio)
    return max_ratio

def phones_to_match(names_list, names_list_2):
    if len(names_list)>0 and len(names_list_2)>0:
        for x in names_list:
            for y in names_list_2:
                if x == y and x != "":
                    return True
    return False


def geo_flatten(lr_df, lr_df_bbl,lr_df_nobbl,col_compile_list):
    # bitchin = 0
    progress_1 = Progress_meter(limit=len(lr_df_bbl.index))
    indexes_dropped = []
    for index, row in lr_df_bbl.iterrows():
        progress_1.display()
        # if bitchin > 100:
        #     break
        # bitchin+=1
        if index not in indexes_dropped:
            group = lr_df_bbl.loc[(lr_df_bbl["BBL"] == row["BBL"]) | (lr_df_bbl["BBL Latest"] == row["BBL Latest"])]
            for index1, row1 in group.iterrows():
                # print(row["Business Name"])
                # print(row1["Business Name"])
                # print(phones_to_match(row["Contact Phone"],row1["Contact Phone"]))
                # print(names_to_match(row["Business Name"],row1["Business Name"]))
                if index != index1 and (phones_to_match(row["Contact Phone"],row1["Contact Phone"]) or names_to_match(row["Business Name"],row1["Business Name"]) > 90):
                    for colname in col_compile_list:
                        lr_df.at[index, colname] = lr_df.loc[index, colname] + row1[colname]
                    indexes_dropped.append(index1)
                    lr_df=lr_df.drop(index1)
                    lr_df_bbl=lr_df_bbl.drop(index1)

    progress_2 = Progress_meter(limit=len(lr_df_nobbl.index))
    for index, row in lr_df_nobbl.iterrows():
        progress_2.display()
        # if bitchin > 100:
        #     break
        # bitchin+=1
        if index not in indexes_dropped:
            #  row["Street"][0]!="nan" and row["Building Number"][0]!="nan" and 
            group = lr_df_nobbl.loc[lr_df_nobbl.apply(lambda df_row: df_row["Building Number"][0]==row["Building Number"][0] and df_row["Street"][0] == row["Street"][0], axis= 1)]
            print(group)
            for index1, row1 in group.iterrows():
                if index != index1 and (phones_to_match(row["Contact Phone"],row1["Contact Phone"]) or names_to_match(row["Business Name"],row1["Business Name"]) > 90):
                    for colname in col_compile_list:
                        lr_df.at[index, colname] = lr_df.loc[index, colname] + row1[colname]
                    indexes_dropped.append(index1)
                    lr_df=lr_df.drop(index1)
                    lr_df_nobbl=lr_df_nobbl.drop(index1)
    
    return lr_df

def begin_process(package):
    if package:
        iacf_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-whole.p","rb"))
        lr_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/lr-residual.p","rb"))

        print(lr_df)
        
        # lr_df = lr_df.apply(lambda x: fix_bbl(x), axis = 1)
        
        lr_df=lr_df.reset_index()
        lr_df.columns = ["Record ID","License Type","License Expirations","License Statuses","License Creations","Building Number","Street","Street 2","City","State","Zip","Contact Phone","Borough","Borough Code","Community Board","Council District","BIN","BBL","NTA","Census Tract","Detail","Longitude","Latitude","Location","RSSs","RSS Dates","Business Name","License Industries","BBL Latest"]
        
        col_compile_list = ["Record ID","License Type","License Expirations","License Statuses","License Creations","Building Number","Street","Street 2","City","State","Zip","Contact Phone","RSSs","RSS Dates","Business Name","License Industries"]

        for colname in ["Record ID","License Type","License Expirations","License Statuses","License Creations","Building Number","Street","Street 2","City","State","Zip","Contact Phone","RSSs","RSS Dates"]:
            lr_df[colname] = lr_df[colname].apply(lambda x: [x])
        
        lr_df['LLID'] = [str(uuid.uuid4()) for _ in range(len(lr_df.index))]
        lr_df['LLID'] = lr_df['LLID'].astype(str)

        lr_df['LBID'] = [str(uuid.uuid4()) for _ in range(len(lr_df.index))]
        lr_df['LBID'] = lr_df['LBID'].astype(str)
        
        lr_df["BBL"] = lr_df["BBL"].apply(lambda a: str(int(float(a))) if (can_be_float(a) and not math.isnan(float(a))) else "")
        lr_df["BBL Latest"] = lr_df["BBL Latest"].apply(lambda a: str(int(float(a))) if (can_be_float(a) and not math.isnan(float(a))) else "")

        print(lr_df['BBL'])
        print(lr_df['BBL Latest'])

        lr_df_bbl = lr_df[~((isblank(lr_df["BBL Latest"])) & (isblank(lr_df["BBL Latest"])))]
        lr_df_nobbl = lr_df[((isblank(lr_df["BBL Latest"])) & (isblank(lr_df["BBL Latest"])))]
        lr_df_nobbl = lr_df_nobbl[lr_df_nobbl.apply(lambda df_row: len(df_row["Building Number"])>df_row["Building Number"].count("nan") and len(df_row["Street"])>df_row["Building Number"].count("nan"), axis= 1)]

        lr_df = geo_flatten(lr_df, lr_df_bbl,lr_df_nobbl,col_compile_list)

        full_pd = pd.concat([lr_df,iacf_df], join='outer',ignore_index=True)

        pickle.dump(lr_df, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/lr-residual-flat.p","wb"))
        pickle.dump(full_pd, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-whole-flat.p","wb"))

        lr_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/lr-residual-flat.csv",index=False,quoting=csv.QUOTE_ALL)
        full_pd.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/dca-whole-flat.csv",index=False,quoting=csv.QUOTE_ALL)


if __name__ == "__main__":
    begin_process(package = True)