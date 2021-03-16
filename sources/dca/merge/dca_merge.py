from global_vars import *

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import uuid
import multiprocessing
from multiprocessing import Process
from multiprocessing import Manager
from functools import reduce

def can_be_float(flt):
    if flt == "":
        return False
    try:
        float(flt)
        return True
    except:
        return False

def sub(m):
    words_ignore = set(["corp", "//$L$//", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon', 'smoke','pizza','of','the'])
    return '' if m.group() in words_ignore else m.group()

def names_to_match(name, name_list):
    max_ratio = 0
    if len(name)>0 and len(name_list)>0:
        for x in name:
            for y in name_list:
                if x != "" and y != "":
                    fuzz_ratio = fuzz.WRatio(re.sub(r'\w+', sub, x.lower()),re.sub(r'\w+', sub, y.lower()))
                    max_ratio = (fuzz_ratio if fuzz_ratio > max_ratio else max_ratio)
    return max_ratio

def phones_to_match(name, name_list):
    if len(name)>0 and len(name_list)>0:
        if name in name_list and name != "":
            return True
    return False

def isblank(input_na):
    return (input_na=="") | (input_na == "nan") | (pd.isna(input_na)) | (pd.isnull(input_na))

def iterate_iacf(iacf_index, iacf_row,iacf_df,lr_df,lr_df_bbl,lr_df_nobbl):
    indexset = []
    indexes_used = []
    group = iacf_df.loc[iacf_df["LBID"] == iacf_row["LBID"]]

    business_names = []
    phone_numbers = []
    record_ids = []

    for name in group["Business Name"].tolist():
        business_names += name
    for phone in group["Contact Phone"].tolist():
        phone_numbers += phone
    for rec in group["Record ID"].tolist():
        record_ids += rec
    
    license_creations = []
    license_expirations = []
    rsss = []
    rss_dates = []
    industries = []
    statuses = []

    for rec in list(set(record_ids)):
        if "DCA" in rec and rec in lr_df.index:
            try:
                business_names+=lr_df.loc[rec, "Business Name"]
                phone_numbers.append(lr_df.loc[rec, "Contact Phone Number"])
                license_creations.append(lr_df.loc[rec, "License Creation Date"])
                license_expirations.append(lr_df.loc[rec, "License Expiration Date"])
                rsss.append(lr_df.loc[rec, "RSS"])
                rss_dates.append(lr_df.loc[rec, "RSS Date"])
                industries+=lr_df.loc[rec, "Industry"]
                statuses.append(lr_df.loc[rec, "License Status"])
            except:
                pass
            lr_df=lr_df.drop(rec)
            indexes_used.append(rec)

    group_lr = lr_df_bbl.loc[((lr_df_bbl["BBL"] == iacf_row["BBL"])) | ((lr_df_bbl["BBL Latest"] == iacf_row["BBL Latest"]))]
    for index, row in group_lr.iterrows():
        if index in lr_df.index and (phones_to_match(row["Contact Phone Number"],iacf_row["Contact Phone"]) or names_to_match(row["Business Name"],iacf_row["Business Name"]) > 90):
            try:
                business_names+=lr_df.loc[index, "Business Name"]
                phone_numbers.append(lr_df.loc[index, "Contact Phone Number"])
                record_ids.append(index)
                license_creations.append(lr_df.loc[index, "License Creation Date"])
                license_expirations.append(lr_df.loc[index, "License Expiration Date"])
                rsss.append(lr_df.loc[index, "RSS"])
                rss_dates.append(lr_df.loc[index, "RSS Date"])
                industries+=lr_df.loc[index, "Industry"]
                statuses.append(lr_df.loc[index, "License Status"])
            except:
                pass
            lr_df=lr_df.drop(index)
            lr_df_bbl=lr_df_bbl.drop(index)
            indexes_used.append(index)
    
    group_lr = lr_df_nobbl.loc[lr_df_nobbl.apply(lambda df_row: df_row["Address Building"]==iacf_row["Building Number"][0] and df_row["Address Street Name"] == iacf_row["Street"][0], axis= 1)]
    for index, row in group_lr.iterrows():
        if index in lr_df.index and (phones_to_match(row["Contact Phone Number"],iacf_row["Contact Phone"]) or names_to_match(row["Business Name"],iacf_row["Business Name"]) > 90):
            try:
                business_names+=lr_df.loc[index, "Business Name"]
                phone_numbers.append(lr_df.loc[index, "Contact Phone Number"])
                record_ids.append(index)
                license_creations.append(lr_df.loc[index, "License Creation Date"])
                license_expirations.append(lr_df.loc[index, "License Expiration Date"])
                rsss.append(lr_df.loc[index, "RSS"])
                rss_dates.append(lr_df.loc[index, "RSS Date"])
                industries+=lr_df.loc[index, "Industry"]
                statuses.append(lr_df.loc[index, "License Status"])
            except:
                pass
            lr_df=lr_df.drop(index)
            lr_df_nobbl=lr_df_nobbl.drop(index)
            indexes_used.append(index)

    for index, row in group.iterrows():
        iacf_df.at[index, "Business Name"] = list(set(business_names))
        iacf_df.at[index, "Contact Phone"] = list(set(phone_numbers))
        iacf_df.at[index, "Record ID"] = list(set(record_ids))
        iacf_df.at[index, "License Creations"] = license_creations
        iacf_df.at[index, "License Expirations"] = license_expirations
        iacf_df.at[index, "License Industries"] = industries
        iacf_df.at[index, "License Statuses"] = statuses
        iacf_df.at[index, "RSSs"] = rsss
        iacf_df.at[index, "RSS Dates"] = rss_dates
        indexset.append(index)
    
    return lr_df, iacf_df, indexset, indexes_used, lr_df_bbl,lr_df_nobbl

def begin_process(thread_count,iacf_df,lr_df,lr_df_bbl,lr_df_nobbl):

    indexes_used = []

    if thread_count == 0:
        progress = Progress_meter(limit=len(iacf_df.index))

    indexset = []

    for iacf_index, iacf_row in iacf_df.iterrows():
        if thread_count == 0:
            progress.display()
        # else:
        #     progress.tick()
        
        # if progress.meter > 10:
        #     break

        if iacf_index not in indexset:
            result = iterate_iacf(iacf_index, iacf_row,iacf_df,lr_df,lr_df_bbl,lr_df_nobbl)
            lr_df = result[0]
            iacf_df = result[1]
            indexset += result[2]
            indexes_used += result[3]
            lr_df_bbl = result[4]
            lr_df_nobbl = result[5]
            

    pickle.dump(iacf_df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-whole-"+str(thread_count)+".p","wb"))
    pickle.dump(indexes_used,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/indexes-used-"+str(thread_count)+".p","wb"))

def begin_threads(thread_num, package):

    # lob_revocations_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/lob_revocations.csv"
    # insp_app_chrg_fnf_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/insp_app_chrg_fnf.csv"
    # lr_df = pd.read_csv(lob_revocations_path)
    # iacf_df = pd.read_csv(insp_app_chrg_fnf_path)

    lr_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/lob_revocations.p","rb"))
    iacf_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/final-df.p", "rb"))

    lr_df = lr_df.set_index('DCA License Number')
    lr_df = lr_df.sort_values("License Creation Date")

    iacf_df['License Creations'] = [[] for _ in range(len(iacf_df))]
    iacf_df['License Expirations'] = [[] for _ in range(len(iacf_df))]
    iacf_df['License Industries'] = [[] for _ in range(len(iacf_df))]
    iacf_df['License Statuses'] = [[] for _ in range(len(iacf_df))]
    iacf_df['RSSs'] = [[] for _ in range(len(iacf_df))]
    iacf_df['RSS Dates'] = [[] for _ in range(len(iacf_df))]
    iacf_df=iacf_df.sort_values("LBID")

    print(iacf_df[iacf_df.duplicated(subset=['LLID'])])


    lr_df["Address Street Name"] = lr_df["Address Street Name"].astype(str)
    lr_df["Address Building"] = lr_df["Address Building"].astype(str)
    lr_df["Contact Phone Number"] = lr_df["Contact Phone Number"].astype(str)

    lr_df["BBL"] = lr_df["BBL"].apply(lambda a: str(int(float(a))) if (can_be_float(a) and not math.isnan(float(a))) else "")
    lr_df["BBL Latest"] = lr_df["BBL Latest"].apply(lambda a: str(int(float(a))) if (can_be_float(a) and not math.isnan(float(a))) else "")

    lr_df_bbl = lr_df[~((isblank(lr_df["BBL Latest"])) & (isblank(lr_df["BBL Latest"])))]
    lr_df_nobbl = lr_df[((isblank(lr_df["BBL Latest"])) & (isblank(lr_df["BBL Latest"])))]
    lr_df_nobbl = lr_df_nobbl[lr_df_nobbl.apply(lambda df_row: len(df_row["Address Building"])>0 and len(df_row["Address Street Name"])>0, axis= 1)]

    if package:

        lbid_list = list(set(iacf_df['LBID'].tolist()))
        divlen = int((len(lbid_list)/thread_num))
        chunks = [lbid_list[x:x+divlen] for x in range(0, len(lbid_list), divlen)]
        chunks[-2] = chunks[-2]+chunks[-1]
        del chunks[-1]

        iacf_df_segments = []
        total_len = 0
        for x in chunks:
            iacf_df_segments.append(iacf_df[iacf_df["LBID"].isin(x)])
            total_len += len(iacf_df_segments[-1])
        
        print("TOTAL LENGTH: " + str(total_len))
        print(len(chunks))

        thread_list = []

        for x in range(0, thread_num):
            thread_list.append(Process(target=begin_process, args=(x,iacf_df_segments[x],lr_df,lr_df_bbl,lr_df_nobbl)))
            
        for x in thread_list:
            x.start()

        for x in thread_list:
            x.join()
    
        print("Finished big process.")
    
    print("Start packaging.")

    iacf_df_list = []
    lr_df_list = []
    for x in range(0,thread_num):
        try:
            iacf_df_list.append(pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-whole-"+str(x)+".p","rb")))
        except:
            print("1" + str(x))
        try:
            lr_df_list+=(pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/indexes-used-"+str(x)+".p","rb")))
        except:
            print("2" + str(x))

    final_iacf_df = pd.concat(iacf_df_list, ignore_index=True)
    final_lr_df = lr_df[~lr_df.index.isin(lr_df_list)]

    print(final_lr_df)

    pickle.dump(final_iacf_df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-whole.p","wb"))
    pickle.dump(final_lr_df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/lr-residual.p","wb"))

    final_iacf_df.to_csv(LOCAL_LOCUS_PATH+"data/whole/dca_files/iacf.csv",index=False,quoting = csv.QUOTE_ALL)
    final_lr_df.to_csv(LOCAL_LOCUS_PATH+"data/whole/dca_files/lr_residual.csv",index=True,quoting = csv.QUOTE_ALL)

if __name__ == "__main__":
    begin_threads(thread_num = 4,package=True)
    # begin_threads(thread_num = 26,package=True)