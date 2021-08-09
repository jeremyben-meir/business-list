####### IMPORTS #######

from scripts.common import DirectoryFields
from fuzzywuzzy import fuzz
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
import math
from scipy import spatial
import itertools
from sklearn import mixture

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

    food = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/food.txt', 'r').readlines())
    aes = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/aes.txt', 'r').readlines())
    retail = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/retail.txt', 'r').readlines())
    laundry = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/laundry.txt', 'r').readlines())
    cars = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/cars.txt', 'r').readlines())
    misc = set(open(f'{DirectoryFields.LOCAL_LOCUS_PATH}data/industries/misc.txt', 'r').readlines())
    dictlist = [food,aes,retail,laundry,cars,misc]

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

    def _compute_vectorized(self, bn0_0, bn0_1, bn1_0, bn1_1):

        conc = pd.Series(list(zip(bn0_0, bn0_1, bn1_0, bn1_1)))
        
        def wratio_apply(x):
            maxlist = [0]
            if not np.linalg.norm(x[0]) == 0.0:
                if not np.linalg.norm(x[2]) == 0.0:
                    maxlist.append(self.calculate_cosine_similarity(x[0],x[2]))
                if not np.linalg.norm(x[3]) == 0.0:
                    maxlist.append(self.calculate_cosine_similarity(x[0],x[3]))
            if not np.linalg.norm(x[1]) == 0.0:
                if not np.linalg.norm(x[2]) == 0.0:
                    maxlist.append(self.calculate_cosine_similarity(x[1],x[2]))
                if not np.linalg.norm(x[3]) == 0.0:
                    maxlist.append(self.calculate_cosine_similarity(x[1],x[3]))
            return max(maxlist)

        return conc.apply(wratio_apply)

class CompareRecordIDs(BaseCompareFeature):

    def _compute_vectorized(self, record_id_0_0, record_id_0_1, record_id_1_0, record_id_1_1):
        sim = ((((record_id_0_0 == record_id_1_0) | (record_id_0_0 == record_id_1_1)) & (~record_id_0_0.isna())) |
               (((record_id_0_1 == record_id_1_0) | (record_id_0_1 == record_id_1_1)) & (~record_id_0_1.isna()))).astype(float)
        return sim

class ComparePhones(BaseCompareFeature):

    def _compute_vectorized(self, phone_0, phone_1):
        sim = (phone_0 == phone_1).astype(float)
        sim[(sim == 0) & (phone_0.isna()|phone_1.isna())] = 0.5
        return sim
        
class Merge():

    def __init__(self):
        self.df = self.load_source_files()

    def text_prepare(self, text, street0):
        STOPWORDS = ["nan","corp", "corp.", 'corporation', "inc", "inc.", "incorporated", "airlines", "llc", "llc.", "laundromat", "laundry", 'deli', 'grocery', 'market', 'wireless', 'auto', 'pharmacy', 'cleaners', 'parking', 'repair', 'electronics','salon','smoke','pizza','of','the', 'retail', 'new', 'news', 'food', 'group']
        if street0:
            street0 = street0.lower()
            if len(street0) > 2:
                STOPWORDS += street0.split(" ")
        text = re.sub(re.compile('[\n\"\'/(){}[]|@,;.#]'), '', text)
        text = re.sub(' +', ' ', text)
        text = text.lower()
        text = ' '.join([word for word in text.split() if word not in STOPWORDS]) 
        text = text.strip()
        return text

    def prepare_business_names(self,row):
        if isinstance(row["Business Name"],str):
            row["bn0"] = self.text_prepare(row["Business Name"],row["Street"])
        if isinstance(row["Business Name 2"],str):
            row["bn1"] = self.text_prepare(row["Business Name 2"],row["Street"])
        return row

    def type_cast(self, df):
        df["Record ID"] = df["Record ID"].astype(str)
        df["Record ID 2"] = df["Record ID 2"].astype(str)
        df["Record ID 3"] = df["Record ID 3"].astype(str)
        df["Street"] = df["Street"].astype(str)
        df["Business Name"] = df["Business Name"].astype(str)
        df["Business Name 2"] = df["Business Name 2"].astype(str)
        df["Contact Phone"] = df["Contact Phone"].astype(str)
        df["Industry"] = df["Industry"].astype(str)
        df = df.replace("", np.nan, regex=True)
        df = df.replace("NaN", np.nan, regex=True)
        df = df.replace("nan", np.nan, regex=True)
        return df

    def load_source_files(self, loaded = True):
        
        if not loaded:
            filelist = [("dca","charge"),("dca","inspection"),("dca","application"),("dca","license"),("doa","inspection"),
                        ("doe","pharmacy"),("doh","inspection"),("dos","license"),("liq","license"),("doh","license"),("dot","application"),("dot","inspection")]
            df_list = [pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{res[0]}/temp/df-{res[1]}-source.p", "rb" )) for res in filelist]

            df = pd.concat(df_list, axis=0, join='outer', ignore_index=False)

            df["LLID"] = ""
            df["LBID"] = ""

            bblmask = (df['BBL'].str.len() == 10) & (df['BBL']!="0000000000") & (df['BBL'].str.isdigit())
            df = df[bblmask]
            df = df.drop_duplicates()
            df = df.reset_index(drop=True)

            df = self.type_cast(df)

            df = df.sort_values(["BBL"])##
            df = df.iloc[:5000]##
            df = df.apply(lambda row: self.prepare_business_names(row), axis=1)

            self.store_pickle(df,0)

        else:

            df = self.load_pickle(0)

        if not df["BBL"].mode().empty:
            common_bbl = df["BBL"].mode().iloc[0]
            sys.setrecursionlimit(max(1000,len(df[df["BBL"] == common_bbl])))
        print(f"recursion depth: {sys.getrecursionlimit()}")

        print("loaded data")
        return df

    def add_llid(self):

        self.df = self.df.reset_index(drop=True)
        self.df = self.type_cast(self.df)

        def ngrams(string, n=3):
            string = re.sub(r'[,-./]|\sBD',r'', string)
            ngrams = zip(*[string[i:] for i in range(n)])
            return [''.join(ngram) for ngram in ngrams]

        def similarity(df):
            df["bn0"] =  df["bn0"].replace(np.nan, "", regex=True)
            df["bn1"] =  df["bn1"].replace(np.nan, "", regex=True)
            vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
            tf_idf_matrix = vectorizer.fit_transform(pd.concat([df["bn0"],df["bn1"]])).toarray()
            tfidf0 = tf_idf_matrix[:int(tf_idf_matrix.shape[0]/2)]
            tfidf1 = tf_idf_matrix[int(tf_idf_matrix.shape[0]/2):]
            df["TFIDF0"] = list(tfidf0)
            df["TFIDF1"] = list(tfidf1)
            return df
        
        self.df = similarity(self.df)

        indexer = recordlinkage.Index()
        indexer.block(on='BBL')
        candidate_links = indexer.index(self.df)

        compare_cl = recordlinkage.Compare()

        compare_cl.add(ComparePhones('Contact Phone','Contact Phone'))
        compare_cl.add(CompareRecordIDs(('Record ID','Record ID 2'),('Record ID','Record ID 2')))
        compare_cl.add(CompareIndustries('Industry','Industry'))
        compare_cl.add(CompareNames(('TFIDF0','TFIDF1'),('TFIDF0','TFIDF1')))

        features = compare_cl.compute(candidate_links, self.df)

        # # Alternate classifier (K-means)
        # classifier = recordlinkage.KMeansClassifier()
        # matches = classifier.fit_predict(features)
        # print(matches)

        g = mixture.GaussianMixture(n_components=2,random_state=43)
        np_to_predict = features.to_numpy()
        result = g.fit_predict(np_to_predict)
        features["pred"] = list(result)
        matches = features[features["pred"]==1].index

        print(features)

        g = Graph(len(self.df.index))
        g.addEdges(matches.tolist())
        cc = g.connectedComponents()
        
        for indexlist in cc:
            self.df.loc[indexlist,"LLID"] = str(uuid.uuid4())

        self.store_pickle(self.df,1)

    def add_lbid(self):
        self.df = self.load_pickle("merged")
        self.df = self.df.reset_index(drop=True)
        exp_llid = set()
        exp_rid = set()
        exp_rid2 = set()
        matches = []
        for _, row in self.df.iterrows():
            subdf = self.df[(self.df.index != row.name) &
                           (((~self.df["LLID"].isin(exp_llid))&(self.df["LLID"]==row["LLID"]))
                           |((~self.df["Record ID"].isin(exp_rid))&(self.df["Record ID"]==row["Record ID"]))
                           |((~self.df["Record ID"].isin(exp_rid2))&(self.df["Record ID 2"]==row["Record ID 2"])))]
            exp_llid.add(row["LLID"])
            exp_rid.add(row["Record ID"])
            exp_rid2.add(row["Record ID 2"])
            matches += [(row.name,val) for val in subdf.index.values.tolist()]
        
        g = Graph(len(self.df.index))
        g.addEdges(matches)
        cc = g.connectedComponents()

        for indexlist in cc:
            self.df.loc[indexlist,"LBID"] = str(uuid.uuid4())

        self.store_pickle(self.df,2)

    def store_pickle(self,df,num):
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-{num}.p", "wb" ))
    
    def load_pickle(self,num):
        return pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/df-{num}.p", "rb" ))

    def save_csv(self):
        # self.df = self.df.sort_values("BBL")
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/merged.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        self.store_pickle(self.df,"merged")

if __name__ == '__main__':
    merge = Merge()

    start = time.time()
    merge.add_llid()
    end = time.time()
    print(end - start)

    # merge.add_lbid()

    merge.save_csv()

    