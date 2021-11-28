from sentence_transformers import SentenceTransformer, util
import pandas as pd
import os
import csv
import pickle
import time
from common import DirectoryFields
import concurrent.futures
import numpy as np
import boto3

class IndustryAssign():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.overwrite = True
        try:
            df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-assigned.p").get()['Body'].read())
            self.df = df[df["NAICS"]==""]
            self.org_df = df[df["NAICS"]!=""]
            self.overwrite = False
        except: 
            self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-merged.p").get()['Body'].read())
        self.ticker = 0
        self.totlen = len(self.df["LBID"].unique())
        # self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-merged.p", "rb" ))

    def industry_assign(self, inp_list):
        model_name = 'paraphrase-MiniLM-L6-v2'
        model = SentenceTransformer(model_name)

        try:
            naics = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/naics.p").get()['Body'].read())
            # naics = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics.p", "rb" ))
            corpus_embeddings = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/naics_encodings.p").get()['Body'].read())
            # corpus_embeddings = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics_encodings.p", "rb" ))
        except:
            naics = pd.read_csv(f"{DirectoryFields.S3_PATH}data/2017_NAICS_Descriptions.csv")
            naics = naics[naics['Code'].str.len() == 6].reset_index(drop=False)
            corpus_sentences = list(naics['Description'])
            corpus_embeddings = model.encode(corpus_sentences, show_progress_bar=True, convert_to_tensor=True)

            path = f'data/temp/naics.p' 
            self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=path, Body=pickle.dumps(naics))
            # pickle.dump(naics , open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics.p", "wb" ))

            path = f'data/temp/naics_encodings.p' 
            self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=path, Body=pickle.dumps(corpus_embeddings))
            # pickle.dump(corpus_embeddings , open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/naics_encodings.p", "wb" ))

        inp_list = list(set(inp_list))
        inp = ''
        for i in inp_list:
            inp+=f'{i} '
        
        question_embedding = model.encode(inp, convert_to_tensor=True)
        hits = util.semantic_search(question_embedding, corpus_embeddings)
        return naics.iloc[hits[0][0]['corpus_id']]

    def split_dataframes(self, df, segment_num, split_col):
        df_list = list()
        col_num = int(df[split_col].nunique()/segment_num)
        for _ in range(segment_num):
            sampled_col = np.random.choice(df[split_col].unique(), col_num)
            df_list.append(df[df[split_col].isin(sampled_col)])
            df = df[~df[split_col].isin(sampled_col)]
        df_list.append(df)
        return df_list

    def apply_st_async(self):
        self.df["NAICS"] = ""
        self.df["NAICS Title"] = ""
        df_list = self.split_dataframes(self.df,15,"LBID")
        future_list = list()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for df in df_list:
                future = executor.submit(self.apply_st, df)
                future_list.append(future)
        self.df = pd.concat([future.result() for future in future_list])

        self.save_df()
        return self.df
    
    def save_df(self):
        path = f'data/temp/df-assigned.p' 
        if self.overwrite:
            self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=path, Body=pickle.dumps(self.df))
        else:
            save_df = pd.concat([self.df,self.org_df],ignore_index=True)
            self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=path, Body=pickle.dumps(save_df))

        
    def apply_st(self, df):
        unique_lbid = sorted(df["LBID"].unique())
        for lbid in unique_lbid:
            industry_list = df.loc[df["LBID"]==lbid,"Industry"].tolist()
            naics_return = self.industry_assign(industry_list)
            naics_code = naics_return.loc["Code"]
            naics_title = naics_return.loc["Title"]
            self.df.loc[self.df["LBID"]==lbid,"NAICS"] = naics_code
            self.df.loc[self.df["LBID"]==lbid,"NAICS Title"] = naics_title
            self.ticker += 1
            print(f"{self.ticker} / {self.totlen}")
            if self.ticker % 200 == 0:
                self.save_df()
            # print(naics_title)
            # print(df.loc[df["LBID"]==lbid,"NAICS Title"])
        return df

if __name__ == "__main__":
    industry_assign = IndustryAssign()
    industry_assign.apply_st_async()
    