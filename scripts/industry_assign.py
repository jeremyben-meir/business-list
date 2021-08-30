from sentence_transformers import SentenceTransformer, util
import pandas as pd
import os
import csv
import pickle
import time
from scripts.common import DirectoryFields
import concurrent.futures
import numpy as np

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
    col_num = int(df[split_col].nunique()/segment_num)
    for _ in range(segment_num):
        sampled_col = np.random.choice(df[split_col].unique(), col_num)
        df_list.append(df[df[split_col].isin(sampled_col)])
        df = df[~df[split_col].isin(sampled_col)]
    df_list.append(df)
    return df_list

def apply_st_async(inp_df):
    inp_df["NAICS"] = ""
    inp_df["NAICS Title"] = ""
    df_list = split_dataframes(inp_df,15,"LBID")
    future_list = list()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for df in df_list:
            future = executor.submit(apply_st, df)
            future_list.append(future.result())
    inp_df = pd.concat(future_list)
    return inp_df
    
def apply_st(df):
    unique_lbid = sorted(df["LBID"].unique)
    for lbid in unique_lbid:
        industry_list = df.loc[df["LBID"]==lbid,"Industry"].tolist()
        naics_return = industry_assign(industry_list)
        naics_code = naics_return.loc["Code"]
        naics_title = naics_return.loc["Title"]
        df.loc[df["LBID"]==lbid,"NAICS"] = naics_code
        df.loc[df["LBID"]==lbid,"NAICS Title"] = naics_title
    return df

if __name__ == "__main__":
    df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-merged.p", "rb" ))
    df = apply_st_async(df)


# print(industry_assign(['Cattle farmer','Cattle farmer','Soy farmer','Milk store']))

    