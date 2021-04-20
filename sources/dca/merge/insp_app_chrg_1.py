#######IMPORTS#######

from global_vars import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import uuid

#######FUNCTION DEFINITIONS#########

def load_source_files():

    df_o1 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-insp-2.p", "rb" ))
    df_o2 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-app-2.p", "rb" ))
    df_o3 = pickle.load( open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-charge-2.p", "rb" ))

    df = pd.concat([df_o1,df_o2,df_o3], axis=0, join='outer', ignore_index=False)

    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/dca-merged-notflat.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
    return df

def group(df):
    df = df.sort_values('BBL')
    # df=df.head(1000)
    group = df.groupby('BBL')
    # df = df.groupby(['Record ID','Building Number', 'Street']).agg(lambda x: list(x)).reset_index()
    print(group)


def begin_process():
    df_0 = load_source_files()
    df_1 = group(df_0)

if __name__ == '__main__':
    begin_process()