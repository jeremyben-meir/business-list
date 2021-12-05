####### IMPORTS #######

from common import DirectoryFields
import pickle
import re
import pandas as pd
import uuid
import csv
import recordlinkage
from recordlinkage.base import BaseCompareFeature
import time
import sys
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy import spatial
from sklearn import mixture
import concurrent.futures
import smart_open
import boto3

class Graph:
 
    # init function to declare class variables
    def __init__(self, V):
        self.V = V
        self.adj = [[] for i in range(V)]
 
    def DFSUtil(self, temp, v, visited):
 
        # Mark the current vertex as visited
        visited[v] = True
 
        # Store the vertex to list
        temp.append(v)
 
        # Repeat for all vertices adjacent
        # to this vertex v
        for i in self.adj[v]:
            if visited[i] == False:
 
                # Update the list
                temp = self.DFSUtil(temp, i, visited)
        return temp
 
    # method to add an undirected edge
    def addEdges(self, edges):
        for edge in edges:
            v,w = edge
            self.adj[v].append(w)
            self.adj[w].append(v)
 
    # Method to retrieve connected components
    # in an undirected graph
    def connectedComponents(self):
        visited = []
        cc = []
        for _ in range(self.V):
            visited.append(False)
        for v in range(self.V):
            if visited[v] == False:
                temp = []
                cc.append(self.DFSUtil(temp, v, visited))
        return cc

class CompareIndustries(BaseCompareFeature):

    # EXAMPLE: food = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/food.txt', 'r').read().splitlines())
    
    dictlist = []
    for filename in ["food","aes","retail","laundry","cars","misc"]:
        path = f"data/industries/{filename}.txt"
        textfile = smart_open.smart_open(DirectoryFields.S3_PATH + path)
        return_list = []
        for line in textfile:
            return_list.append(line.decode('utf-8').strip("\n"))
        dictlist.append(set(return_list))

    def _compute_vectorized(self, industry_0, industry_1):
        mask_list = False
        for x in self.dictlist:
            mask_list |= ((industry_0.isin(x)) & (industry_1.isin(x)))
        return mask_list.astype(float)

class CompareNames(BaseCompareFeature):

    def calculate_cosine_distance(self, a, b):
        cosine_distance = float(spatial.distance.cosine(a, b))
        return cosine_distance

    def calculate_cosine_similarity(self,a, b):
        cosine_similarity = 1 - self.calculate_cosine_distance(a, b)
        return cosine_similarity

    def _compute_vectorized(self, bn0_0, bn0_1, bn0_2, bn1_0, bn1_1, bn1_2):

        conc = pd.Series(list(zip(bn0_0, bn0_1, bn0_2, bn1_0, bn1_1, bn1_2)))
        
        def wratio_apply(x):
            maxlist = [0]
            for indx in [0,1,2]:
                if not np.linalg.norm(x[indx]) == 0.0:
                    for indx2 in [3,4,5]: 
                        if not np.linalg.norm(x[indx2]) == 0.0:
                            maxlist.append(self.calculate_cosine_similarity(x[indx],x[indx2]))
            return max(maxlist)

        return conc.apply(wratio_apply)

class CompareRecordIDs(BaseCompareFeature):

    def _compute_vectorized(self, record_id_0_0, record_id_0_1, record_id_0_2, record_id_1_0, record_id_1_1, record_id_1_2):
        sim = ((((record_id_0_0 == record_id_1_0) | (record_id_0_0 == record_id_1_1) | (record_id_0_0 == record_id_1_2)) & (~record_id_0_0.isna())) |
               (((record_id_0_1 == record_id_1_0) | (record_id_0_1 == record_id_1_1) | (record_id_0_1 == record_id_1_2)) & (~record_id_0_1.isna())) |
               (((record_id_0_2 == record_id_1_0) | (record_id_0_2 == record_id_1_1) | (record_id_0_2 == record_id_1_2)) & (~record_id_0_2.isna()))).astype(float)
        return sim

class ComparePhones(BaseCompareFeature):

    def _compute_vectorized(self, phone_0, phone_1):
        sim = (phone_0 == phone_1).astype(float)
        sim[(sim == 0) & (phone_0.isna()|phone_1.isna())] = 0.5
        return sim
        
class Merge():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.df = self.load_source_files(loaded=True)
        self.split_num = 100

    def split_dataframes(self, df, segment_num, split_col):
        df_list = list()
        segment_num -= 1
        if segment_num > 0:
            col_num = int(df[split_col].nunique()/segment_num)
            for _ in range(segment_num):
                sampled_col = np.random.choice(df[split_col].unique(), col_num)
                df_list.append(df[df[split_col].isin(sampled_col)])
                df = df[~df[split_col].isin(sampled_col)]
        df_list.append(df)
        return df_list

    def text_prepare(self, text, street0):
        STOPWORDS = ["nan","corp", "corp.", 'corporation', "inc", "inc.", "incorporated", "llc", "llc." 'group']
        if street0:
            street0 = street0.lower()
            if len(street0) > 2:
                STOPWORDS += street0.split(" ")
        text = re.sub(r"[^a-zA-Z0-9]+", ' ', text)
        text = text.lower()
        text = ' '.join([word for word in text.split() if word not in STOPWORDS]) 
        text = text.strip()
        return text

    def prepare_business_names(self,row):
        if isinstance(row["Business Name"],str):
            row["bn0"] = self.text_prepare(row["Business Name"],row["Street"])
        if isinstance(row["Business Name 2"],str):
            row["bn1"] = self.text_prepare(row["Business Name 2"],row["Street"])
        if isinstance(row["Business Name 3"],str):
            row["bn2"] = self.text_prepare(row["Business Name 3"],row["Street"])
        return row

    def type_cast(self, df, replace_nan = True):
        df["Record ID"] = df["Record ID"].astype(str)
        df["Record ID 2"] = df["Record ID 2"].astype(str)
        df["Record ID 3"] = df["Record ID 3"].astype(str)
        df["Street"] = df["Street"].astype(str)
        df["Business Name"] = df["Business Name"].astype(str)
        df["Business Name 2"] = df["Business Name 2"].astype(str)
        df["Business Name 3"] = df["Business Name 3"].astype(str)
        df["Contact Phone"] = df["Contact Phone"].astype(str)
        df["Industry"] = df["Industry"].astype(str)
        if replace_nan:
            df = df.replace("", np.nan, regex=True)
            df = df.replace("NaN", np.nan, regex=True)
            df = df.replace("nan", np.nan, regex=True)
        return df

    def load_source_files(self, loaded = True):
        
        if not loaded:
            filelist = [
                ("dca","charge"),
                ("dca","inspection"),
                ("dca","application"),
                ("dca","license"),
                ("doa","inspection"),
                ("doe","pharmacy"),
                ("doh","inspection"),
                ("dos","license"),
                ("liq","license"),
                ("doh","license"),
                ("dot","application"),
                ("dot","inspection"),
                ("dof","certificate"),
                ]
            
            def load_file(path):
                print(path)
                return pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(path).get()['Body'].read()) 

            df_list = [load_file(f"data/{res[0]}/temp/df-{res[1]}-source.p") for res in filelist]

            df = pd.concat(df_list, axis=0, join='outer', ignore_index=False)

            df["LLID"] = ""
            df["LBID"] = ""
            df["bn0"] = ""
            df["bn1"] = ""
            df["bn2"] = ""

            bblmask = (df['BBL'].str.len() == 10) & (df['BBL']!="0000000000") & (df['BBL'].str.isdigit())
            df = df[bblmask]

            print(df)

            df = df.drop_duplicates()
            df = df.reset_index(drop=True)
            df = self.type_cast(df, replace_nan = False)
            df = df.apply(lambda row: self.prepare_business_names(row), axis=1)

            self.store_pickle(df,0)

        else:

            df = self.load_pickle(0)

        df.reset_index(drop=True)
        leng = len(df)
        
        # df = df[df["Zip"].isin(["10028", "10013", "11372", "11213", "10458", "10304"])]
        # df = df[df["Zip"].isin(["10025"])]

        print()
        print(f"Loaded data with {len(df)}/{leng} rows")

        if not df["BBL"].mode().empty:
            common_bbl = df["BBL"].mode().iloc[0]
            sys.setrecursionlimit(max(1000,len(df[df["BBL"] == common_bbl])))
        print(f"Recursion depth: {sys.getrecursionlimit()}")

        return df

    def add_llid_async(self):

        df_list = self.split_dataframes(self.df,self.split_num,"BBL")
        print("Dataframes split")
        result_df = None
        print()
        
        ticker = 1
        for df in df_list:
            print(f"{ticker} / {self.split_num} Dataframe: {len(df)}")
            llid_df = self.add_llid(df)
            if result_df is None:
                result_df = llid_df
            else:
                result_df = result_df.append(llid_df, ignore_index=True)
            ticker += 1

        self.store_pickle(result_df,1)
        print(result_df)

    def add_llid(self, df, depth = 0):

        df = df.reset_index(drop=True)
        df = self.type_cast(df)

        def ngrams(string, n=3):
            string = re.sub(r'[,-./]|\sBD',r'', string)
            ngrams = zip(*[string[i:] for i in range(n)])
            return [''.join(ngram) for ngram in ngrams]

        def similarity(df):
            df["bn0"] =  df["bn0"].replace(np.nan, "", regex=True)
            df["bn1"] =  df["bn1"].replace(np.nan, "", regex=True)
            df["bn2"] =  df["bn2"].replace(np.nan, "", regex=True)
            vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
            tf_idf_matrix = vectorizer.fit_transform(pd.concat([df["bn0"],df["bn1"],df["bn2"]])).toarray()
            tfidf0 = tf_idf_matrix[:int(tf_idf_matrix.shape[0]/3)]
            tfidf1 = tf_idf_matrix[int(tf_idf_matrix.shape[0]/3):2*int(tf_idf_matrix.shape[0]/3)]
            tfidf2 = tf_idf_matrix[2*int(tf_idf_matrix.shape[0]/3):]
            df["TFIDF0"] = list(tfidf0)
            df["TFIDF1"] = list(tfidf1)
            df["TFIDF2"] = list(tfidf2)
            del tf_idf_matrix
            return df
        
        print("vectorizing")
        try:
            df = similarity(df)

            print("blocking")
            indexer = recordlinkage.Index()
            indexer.block(on='BBL')
            candidate_links = indexer.index(df)

            compare_cl = recordlinkage.Compare()

            compare_cl.add(ComparePhones('Contact Phone','Contact Phone'))
            compare_cl.add(CompareRecordIDs(('Record ID','Record ID 2','Record ID 3'),('Record ID','Record ID 2','Record ID 3')))
            compare_cl.add(CompareIndustries('Industry','Industry'))
            compare_cl.add(CompareNames(('TFIDF0','TFIDF1','TFIDF2'),('TFIDF0','TFIDF1','TFIDF2')))

            print("computing")
            features = compare_cl.compute(candidate_links, df)

            del df["TFIDF0"]
            del df["TFIDF1"]
            del df["TFIDF2"]
            
            print("classifying")
            def make_prediction(row):
                if row[0] == 1:
                    return 1
                if row[1] == 1:
                    return 1
                if row[3] > .75:
                    return 1
                if row[2] == 1 and row[3] > .5:
                    return 1
                return 0

            features["pred"] = features.apply(lambda row: make_prediction(row), axis=1)
            matches = features[features["pred"]==1].index

            # print(features)
            # print(matches)

            g = Graph(len(df.index))
            g.addEdges(matches.tolist())
            cc = g.connectedComponents()
            
            for indexlist in cc:
                df.loc[indexlist,"LLID"] = str(uuid.uuid4())
            
            del g
            del features

        except:

            print(f"MINIMIZING, depth {depth}")
            df1, df2 = self.split_dataframes(df,2,"BBL")
            df1 = self.add_llid(df1,depth+1)
            df2 = self.add_llid(df2,depth+1)
            df = pd.concat([df1,df2])
            df = df.reset_index(drop=True)

        if depth == 0:
            print("Completed\n")

        return df

    def add_lbid(self, skip=False):
        self.df = self.load_pickle(1)
        self.df = self.df.reset_index(drop=True)
        self.df = self.type_cast(self.df)

        if not skip:
            exp_llid = set()
            exp_rid = set()
            exp_rid2 = set()
            matches = []
            for _, row in self.df.iterrows():
                subdf = self.df[(self.df.index != row.name) &
                            (((~self.df["LLID"].isin(exp_llid))&(self.df["LLID"]==row["LLID"]))
                            |((~self.df["Record ID"].isin(exp_rid))&(self.df["Record ID"]==row["Record ID"]))
                            |((~self.df["Record ID 2"].isin(exp_rid2))&(self.df["Record ID 2"]==row["Record ID 2"]))
                            |((~self.df["Record ID 3"].isin(exp_rid2))&(self.df["Record ID 3"]==row["Record ID 3"])))]
                exp_llid.add(row["LLID"])
                exp_rid.add(row["Record ID"])
                exp_rid2.add(row["Record ID 2"])
                matches += [(row.name,val) for val in subdf.index.tolist()]
            
            g = Graph(len(self.df.index))
            g.addEdges(matches)
            cc = g.connectedComponents()

            for indexlist in cc:
                self.df.loc[indexlist,"LBID"] = str(uuid.uuid4())
        else:
            self.df["LBID"] = self.df["LLID"] 

        self.store_pickle(self.df,2)

    def store_pickle(self,df,num):
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"data/temp/df-{num}.p", Body=pickle.dumps(df))
        
    def load_pickle(self,num):
        return pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(f"data/temp/df-{num}.p").get()['Body'].read())

    def save_csv(self):
        cleaned_file_path = f"{DirectoryFields.S3_PATH}data/temp/merged.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        self.store_pickle(self.df,"merged")

if __name__ == '__main__':
    merge = Merge()

    start = time.time()
    merge.add_llid_async()
    end = time.time()
    print(f"LLID adding: {end - start} seconds")

    start = time.time()
    merge.add_lbid(skip=True)
    end = time.time()
    print(f"LBID adding: {end - start} seconds")
    
    merge.save_csv()