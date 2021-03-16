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

def match_names(liq_names, liq_copy_names):
    max_ratio = 0
    if len(liq_names)>0 and len(liq_copy_names)>0:
        for x in liq_names:
            for y in liq_copy_names:
                if x != "" and y != "":
                    fuzz_ratio = fuzz.WRatio(re.sub(r'\w+', sub, x.lower()),re.sub(r'\w+', sub, y.lower()))
                    max_ratio = (fuzz_ratio if fuzz_ratio > max_ratio else max_ratio)
    return max_ratio

def add_to_liq_row(liq_row, sub_liq, matched):
    # print(sub_liq)
    for index, row in sub_liq.iterrows():
        if matched or match_names(liq_row["Premises Name"]+liq_row["Trade Name"],[row["Premises Name"],row["Trade Name"]]) > 90:
            for x in sub_liq.columns:
                liq_row[x].append(row[x])
    return liq_row

def big_flatten(liq_row, liq_df_copy, thread_index, progress):
    if thread_index == 0 or thread_index != 31:
        progress.tick(display=True)
    else:
        progress.tick()
    
    if not is_list_nan(liq_row["Serial"]):
        sub_liq = liq_df_copy[(liq_df_copy["Serial"]!="") & (liq_df_copy["Serial"].isin(liq_row["Serial"]))]
        liq_row = add_to_liq_row(liq_row, sub_liq, matched=True)
    
    if not is_list_nan(liq_row["BBL"]):
        sub_liq = liq_df_copy[(liq_df_copy["BBL"]!="") & ((liq_df_copy["BBL"].isin(liq_row["BBL"])) | (liq_df_copy["BBL Latest"].isin(liq_row["BBL"])))]
        liq_row = add_to_liq_row(liq_row, sub_liq, matched=False)
    
    if not is_list_nan(liq_row["BBL Latest"]):
        sub_liq = liq_df_copy[(liq_df_copy["BBL Latest"]!="") & ((liq_df_copy["BBL"].isin(liq_row["BBL Latest"])) | (liq_df_copy["BBL Latest"].isin(liq_row["BBL Latest"])))]
        liq_row = add_to_liq_row(liq_row, sub_liq, matched=False)
    
    if not is_list_nan(liq_row["Address Tab"]):
        sub_liq = liq_df_copy[(liq_df_copy["Address Tab"]!="") & ((liq_df_copy["Address Tab"].isin(liq_row["Address Tab"])))]
        liq_row = add_to_liq_row(liq_row, sub_liq, matched=False)

    return liq_row

def parse(liq_df_sub, liq_df_copy, thread_index):
    progress = Progress_meter(limit=len(liq_df_sub.index))
    liq_df_sub = liq_df_sub.apply(lambda liq_row: big_flatten(liq_row, liq_df_copy, thread_index, progress), axis=1)
    pickle.dump(liq_df_sub,open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-"+str(thread_index)+".p","wb"))
    return

def begin_process(package, thread_num):
    if package:
        liq_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-4.p", "rb"))
        liq_df = liq_df.sample(frac=1).reset_index(drop=True)

        liq_df_copy = liq_df

        # for x in liq_df.columns:
        liq_df = liq_df.applymap(lambda x: [x])

        
        print(liq_df)

        liq_df_sub_list = np.array_split(liq_df, thread_num)
        thread_list = []
        thread_index = 0

        for liq_df_sub in liq_df_sub_list:
            thread_list.append(Process(target=parse, args=(liq_df_sub, liq_df_copy, thread_index)))
            thread_index += 1
            
        for x in thread_list:
            x.start()

        for x in thread_list:
            x.join()

        liq_list = []
        for x in range(0,thread_num):
            try:
                liq_list.append(pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-"+str(x)+".p","rb")))
            except:
                print(x)
        
        liq_list_df = pd.concat(liq_list, ignore_index=True)
        pickle.dump(liq_list_df,open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-flattened.p","wb"))
        liq_list_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-flattened.csv", index=False, quoting=csv.QUOTE_ALL)
    
    my_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-flattened.p","rb"))
    
    # my_df = my_df.drop_duplicates(subset=['Serial'])

    my_df = my_df.applymap(lambda x: list(set(x)))
    my_df["Serial"].sort_values()
    my_df = my_df.loc[my_df["Serial"].astype(str).drop_duplicates().index]
    
    pickle.dump(my_df,open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-flattened-cleaned.p","wb"))
    my_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/liq-flattened.csv", index=False)


if __name__ == "__main__":
    begin_process(package = False, thread_num = multiprocessing.cpu_count())