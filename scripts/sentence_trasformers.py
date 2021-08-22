from sentence_transformers import SentenceTransformer, util
import pandas as pd
import os
import csv
import pickle
import time
from scripts.common import DirectoryFields
import concurrent.futures

def industry_assign(inp_list):
    model_name = 'paraphrase-MiniLM-L6-v2'
    model = SentenceTransformer(model_name)

    try:
        naics = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics.p", "rb" ))
        corpus_embeddings = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics_encodings.p", "rb" ))
    except:
        naics = pd.read_csv(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/2017_NAICS_Descriptions.csv")
        naics = naics[naics['Code'].str.len() == 6].reset_index(drop=False)
        corpus_sentences = list(naics['Description'])
        corpus_embeddings = model.encode(corpus_sentences, show_progress_bar=True, convert_to_tensor=True)

        pickle.dump(naics , open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics.p", "wb" ))
        pickle.dump(corpus_embeddings , open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics_encodings.p", "wb" ))

    inp_list = list(set(inp_list))
    inp = ''
    for i in inp_list:
        inp+=f'{i} '
    
    question_embedding = model.encode(inp, convert_to_tensor=True)
    hits = util.semantic_search(question_embedding, corpus_embeddings)
    return naics.iloc[hits[0][0]['corpus_id']]

def split_dataframes(df, segment_num, split_col):
    df_list = list()
    col_list = sorted(df[split_col].unique())
    segment_size = len(df.index)/segment_num
    print(f"Segment size {segment_size}")

    while len(col_list)>0:
        sub_df = pd.DataFrame(columns=df.columns)
        sub_col_list = list()
        while len(sub_df.index) < segment_size:
            if len(col_list)>0:
                sub_col_list.append(col_list[0])
                col_list.pop(0)
            else:
                break
            sub_df = df[df[split_col].isin(sub_col_list)]
        df_list.append(sub_df)
    return df_list

def apply_st_async(df):
    df_list = split_dataframes(df,15,"LBID")
    future_list = list()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for df in df_list:
            future = executor.submit(apply_st, df)
            future_list.append(future.result())
        df = pd.concat(future_list)
    return df
    
def apply_st(df):
    df["NAICS"] = ""
    df["NAICS Title"] = ""
    unique_lbid = sorted(df["LBID"].unique)
    for lbid in unique_lbid:
        industry_list = df.loc[df["LBID"]==lbid,"Industry"].tolist()
        naics_return = industry_assign(industry_list)
        naics_code = naics_return.loc["Code"]
        naics_title = naics_return.loc["Title"]
        df.loc[df["LBID"]==lbid,"NAICS"] = naics_code
        df.loc[df["LBID"]==lbid,"NAICS Title"] = naics_title
    return df

# print(industry_assign(['Cattle farmer','Cattle farmer','Soy farmer','Milk store']))

    