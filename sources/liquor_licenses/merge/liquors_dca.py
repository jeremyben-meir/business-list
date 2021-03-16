from global_vars import *

import multiprocessing
from multiprocessing import Process
from multiprocessing import Manager

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def is_nan(val):
    return val == "" or val == "nan" or pd.isna(val) or pd.isnull(val)

def is_list_nan(val):
    for x in val:
        if not is_nan(x):
            return False
    return True

def sub(m):
    words_ignore = set(["corp", "//$L$//", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon', 'smoke','pizza','of','the'])
    return '' if m.group() in words_ignore else m.group()

def match_names(liq_names, dca_names):
    max_ratio = 0
    if len(liq_names)>0 and len(dca_names)>0:
        for x in liq_names:
            for y in dca_names:
                if x != "" and y != "":
                    fuzz_ratio = fuzz.WRatio(re.sub(r'\w+', sub, x.lower()),re.sub(r'\w+', sub, y.lower()))
                    max_ratio = (fuzz_ratio if fuzz_ratio > max_ratio else max_ratio)
    return max_ratio

def add_to_dca_row(dca_row, sub_liq):
    for index, row in sub_liq.iterrows():
        if match_names(row["Premises Name"] + row["Trade Name"],dca_row["Business Name"]) > 90:
            for x in sub_liq.columns:
                dca_row["Liquor " + x] += row[x]
            return dca_row, index
    return dca_row, -1

def add_liq_to(dca_row, liq_df, thread_index, progress):
    if thread_index == 0:
        progress.display()
    else:
        progress.tick()
    
    if progress.meter == 1:
        ilist = []
    else:
        ilist = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-ilist-"+str(thread_index)+".p","rb"))

    # print(dca_row["BBL"])
    # print(dca_row["BBL Latest"])
    
    if not is_nan(dca_row["BBL"]):
        sub_liq = liq_df[liq_df["BBL"].apply(lambda x: dca_row["BBL"] in x)]
        response = add_to_dca_row(dca_row, sub_liq)
        dca_row = response[0]
        ilist.append(response[1])
    if not is_nan(dca_row["BBL Latest"]):
        sub_liq = liq_df[liq_df["BBL Latest"].apply(lambda x: dca_row["BBL Latest"] in x)]
        response = add_to_dca_row(dca_row, sub_liq)
        dca_row = response[0]
        ilist.append(response[1])
    if not is_list_nan(dca_row["Building Number"]) and not is_list_nan(dca_row["Street"]) and not is_list_nan(dca_row["Zip"]):
        sub_liq = liq_df[liq_df["Address Tab"].apply(lambda x: any((dca_row["Building Number"][y] in x and dca_row["Zip"][y] in x) for y in range(len(dca_row["Building Number"])) ))]
        response = add_to_dca_row(dca_row, sub_liq)
        dca_row = response[0]
        ilist.append(response[1])

    ilist = pickle.dump(list(set(ilist)),open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-ilist-"+str(thread_index)+".p","wb"))
    
    return dca_row

def parse(dca_df_sub, liq_df, thread_index):
    progress = Progress_meter(limit=len(dca_df_sub.index))
    dca_df_sub = dca_df_sub.apply(lambda dca_row: add_liq_to(dca_row, liq_df, thread_index, progress), axis=1)
    pickle.dump(dca_df_sub,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-"+str(thread_index)+".p","wb"))
    return

def begin_process(package, thread_num):
    liq_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-flattened-cleaned.p", "rb"))
    dca_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-whole-flat-penult.p", "rb"))

    for col in liq_df.columns:
        dca_df["Liquor " + col] = [[] for _ in range(len(dca_df))]
    
    print(liq_df["BBL"])

    dca_df_sub_list = np.array_split(dca_df, thread_num)
    thread_list = []
    thread_index = 0

    # print(dca_df_sub_list[0]["BBL"])

    for dca_df_sub in dca_df_sub_list:
        thread_list.append(Process(target=parse, args=(dca_df_sub, liq_df, thread_index)))
        thread_index += 1
        
    for x in thread_list:
        x.start()

    for x in thread_list:
        x.join()

    dca_list = []
    i_list = []
    for x in range(0,thread_num):
        try:
            dca_list.append(pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-"+str(x)+".p","rb")))
        except:
            print(x)

        try:
            i_list.append(pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-ilist-"+str(x)+".p","rb")))
        except:
            print(x)
    
    dca_list_df = pd.concat(dca_list, ignore_index=True)
    liq_df_residual = liq_df[~liq_df.index.isin(i_list)]

    pickle.dump(dca_list_df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-final.p","wb"))
    pickle.dump(liq_df_residual,open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-residual-final.p","wb"))

    dca_list_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/dca-liq-final.csv", index=False, quoting=csv.QUOTE_ALL)
    liq_df_residual.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-residual-final.csv", index=False, quoting=csv.QUOTE_ALL)
        


if __name__ == "__main__":
    begin_process(package = True, thread_num = multiprocessing.cpu_count()-1)