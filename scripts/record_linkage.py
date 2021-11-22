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
        self.df = self.load_source_files()

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

        # df = df.sort_values("BBL", ignore_index=True)
        # # print(df["source"])
        # df_list = list()
        # segment_size = len(df.index)/segment_num
        # print(f"Segment size {segment_size}")
        
        # while len(df.index)>int(segment_size):
        #     sub_df = df.iloc[:int(segment_size)]
        #     df = df.iloc[int(segment_size):]
        #     df = df.reset_index(drop=True)
        #     sub_df = sub_df.reset_index(drop=True)
        #     end_bbl = sub_df.iloc[-1]["BBL"]
        #     sub_df = sub_df.append(df.loc[df["BBL"] == end_bbl])
        #     df = df[df["BBL"] != end_bbl]
        #     df_list.append(sub_df)
        # df_list.append(df)
        # print(df_list)
        # return df_list

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
        return row

    def type_cast(self, df, replace_nan = True):
        df["Record ID"] = df["Record ID"].astype(str)
        df["Record ID 2"] = df["Record ID 2"].astype(str)
        df["Record ID 3"] = df["Record ID 3"].astype(str)
        df["Street"] = df["Street"].astype(str)
        df["Business Name"] = df["Business Name"].astype(str)
        df["Business Name 2"] = df["Business Name 2"].astype(str)
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
                ("dot","inspection")
                ]
            
            # df_list = [pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{res[0]}/temp/df-{res[1]}-source.p", "rb" )) for res in filelist]
            df_list = [pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(f"data/{res[0]}/temp/df-{res[1]}-source.p").get()['Body'].read()) for res in filelist]

            df = pd.concat(df_list, axis=0, join='outer', ignore_index=False)

            df["LLID"] = ""
            df["LBID"] = ""
            df["bn0"] = ""
            df["bn1"] = ""

            bblmask = (df['BBL'].str.len() == 10) & (df['BBL']!="0000000000") & (df['BBL'].str.isdigit())
            df = df[bblmask]
            df = df[df["Zip"].isin(["10028", "10013", "11372", "11213", "10458", "10304"])]
            print(df)
            df = df.drop_duplicates()

            df = df.reset_index(drop=True)

            df = self.type_cast(df, replace_nan = False)

            # df = df.sort_values(["BBL"])##
            # df = df.iloc[:10000]##
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

    def add_llid_async(self):
        
        df_list = self.split_dataframes(self.df,1,"BBL")
        print("Dataframes split")
        future_list = list()
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     for df in df_list:
        #         future = executor.submit(self.add_llid, df)
        #         future_list.append(future)
        # self.df = pd.concat([future.result() for future in future_list])
        
        for df in df_list:
            future_list.append(self.add_llid(df))
        self.df = pd.concat(future_list)

        self.store_pickle(self.df,1)
        print(self.df)

    def add_llid(self, df):

        df = df.reset_index(drop=True)
        df = self.type_cast(df)

        # process = psutil.Process(os.getpid())
        # print(psutil.cpu_percent(4))
        # process = psutil.Process(os.getpid())
        # print(process.memory_info().rss/(10.0**9))

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
        
        print("vectorizing")
        df = similarity(df)

        print("blocking")
        indexer = recordlinkage.Index()
        indexer.block(on='BBL')
        candidate_links = indexer.index(df)

        compare_cl = recordlinkage.Compare()

        compare_cl.add(ComparePhones('Contact Phone','Contact Phone'))
        compare_cl.add(CompareRecordIDs(('Record ID','Record ID 2','Record ID 3'),('Record ID','Record ID 2','Record ID 3')))
        compare_cl.add(CompareIndustries('Industry','Industry'))
        compare_cl.add(CompareNames(('TFIDF0','TFIDF1'),('TFIDF0','TFIDF1')))

        print("computing")
        features = compare_cl.compute(candidate_links, df)

        del df["TFIDF0"]
        del df["TFIDF1"]
        
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

        ################################################

        # # Alternate classifier (K-means)
        # classifier = recordlinkage.KMeansClassifier()
        # matches = classifier.fit_predict(features)
        # print(matches)

        ################################################

        # np_to_predict = features.to_numpy()

        # g = mixture.GaussianMixture(n_components=2,random_state=43,init_params='random')
        # result = g.fit_predict(np_to_predict)

        # features["pred"] = list(result)
        # features["business 0 n0"] = ""
        # features["business 1 n0"] = ""
        # features["business 0 n1"] = ""
        # features["business 1 n1"] = ""
        # def setindex(row):
        #     row["business 0 n0"] = df.loc[row.name[0], "bn0"]
        #     row["business 1 n0"] = df.loc[row.name[1], "bn0"]
        #     row["business 0 n1"] = df.loc[row.name[0], "bn1"]
        #     row["business 1 n1"] = df.loc[row.name[1], "bn1"]
        #     return row
        # features = features.apply(lambda row: setindex(row),axis=1)
        # cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/temp/features.csv"
        # features.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

        # match_val = 1 - (features["pred"].mode().iloc[0])
        # print(f"MATCHING ON {match_val}")
        # matches = features[features["pred"]==match_val].index

        ###########################################

        print(features)
        print(matches)

        g = Graph(len(df.index))
        g.addEdges(matches.tolist())
        cc = g.connectedComponents()
        
        for indexlist in cc:
            df.loc[indexlist,"LLID"] = str(uuid.uuid4())

        return df

    def add_lbid(self):
        self.df = self.load_pickle(1)
        self.df = self.df.reset_index(drop=True)
        self.df = self.type_cast(self.df)

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
    merge.add_lbid()
    end = time.time()
    print(f"LBID adding: {end - start} seconds")
    
    merge.save_csv()