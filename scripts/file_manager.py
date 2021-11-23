import pandas as pd
from datetime import date
from scripts.common import DirectoryFields
import os
from os import listdir
from os.path import isfile, join
import pickle
import csv
import requests
import time
import glob
import smart_open
import boto3


class FileManager:

    def __init__(self, department, filenames, outname):
        self.s3 = boto3.resource('s3')
        self.filenames = filenames
        self.department = department
        self.df_list = []
        self.outname = outname
        self.update_relevance()
        

    def read_txt(self, path):
        textfile = smart_open.smart_open(path)
        colname = True
        listlist = []
        for line in textfile:
            if colname:
                colnames = line.decode('utf-8').strip("\n").split("\t")
                colname = False
            else:
                listlist.append(line.decode('utf-8').strip("\n").split("\t"))
        return_df = pd.DataFrame(listlist, columns=colnames)
        return return_df

    def get_path_list(self,filename):
        # path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/{filename}/"
        # path_list = [f"{path}{f}" for f in listdir(path) if isfile(join(path, f)) and f[0] != '.']
        path_list = []
        path = f"data/{self.department}/{filename}/"
        for object_summary in self.s3.Bucket(DirectoryFields.S3_PATH_NAME).objects.filter(Prefix=path):
            key_val = object_summary.key
            if key_val != path and "/." not in key_val:
                path_list.append(key_val)
        return path_list

    def get_df_list(self,filename):
        df_list = []

        path_list = self.get_path_list(filename)
        for path in path_list:
            if path[-4:] == '.txt':
                df_list.append(self.read_txt(f"{DirectoryFields.S3_PATH}{path}"))
            else:
                df_list.append(pd.read_csv(f"{DirectoryFields.S3_PATH}{path}", low_memory=False))

        return df_list
    
    def fetch_api(self, url, filename):
        req = requests.get(url)
        url_content = req.content
        filepath = f'data/{self.department}/{filename}/{self.department}_{filename}_{date.today()}.csv'
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=filepath, Body=url_content)
        if self.department == "doh" and filename == "inspection":
            df = pd.read_csv(f"{DirectoryFields.S3_PATH}{filepath}")
            for date_col in ["record_date","grade_date","inspection_date"]:  
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                df[date_col] = df[date_col].apply(lambda date: pd.to_datetime("today") if date.year == 1900 else date)
            df.to_csv(f'{DirectoryFields.S3_PATH}{filepath}', index=False, quoting=csv.QUOTE_ALL)

        # csv_file = open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/{filename}/{self.department}_{filename}_{date.today()}.csv", 'wb')
        # csv_file.write(url_content)
        # csv_file.close()

    def update_relevance(self):
        df = self.get_df()
        
        for filename in self.filenames:
            path = f"data/{self.department}/{filename}/"
            if len(self.get_path_list(filename)) > 0:
                latest_time = min([object_summary.last_modified.date() for object_summary in self.s3.Bucket(DirectoryFields.S3_PATH_NAME).objects.filter(Prefix=path)])
                df.loc[(self.department,filename),'last_retrieved'] = latest_time
            else:
                df.loc[(self.department,filename),'last_retrieved'] = None
        df.to_csv(f"{DirectoryFields.S3_PATH}data/data-relevance.csv")

    def get_df(self):
        df = pd.read_csv(f"{DirectoryFields.S3_PATH}data/data-relevance.csv")
        df = df.set_index(['department','filename'])
        df['last_retrieved']=df['last_retrieved'].astype('datetime64[D]')
        df['api']=df['api'].astype(str)
        df['scrape_py']=df['scrape_py'].astype(str)
        df['FOIL']=df['FOIL'].astype(str)
        return df

    def check_relevance(self,filename):
        df = self.get_df()
        row = df.loc[(self.department,filename)]
        exp_date = row['last_retrieved'] + pd.Timedelta(days=row['retrieval_freq'])
        if exp_date <= date.today() or exp_date is pd.NaT:
            if len(row['api']) > 0 and row['api'] != 'nan':
                print(f"Fetching from {row['api']}")
                self.fetch_api(row['api'],filename)
            elif len(row['scrape_py']) > 0 and row['scrape_py'] != 'nan':
                print(f"Running {row['scrape_py']}")
            elif len(row['FOIL']) > 0 and row['FOIL'] != 'nan':
                print(row['FOIL'])
            else:
                raise ValueError("No source given.")
            return False

    def retrieve_df(self):
        for filename in self.filenames:
            self.check_relevance(filename)
            self.df_list += self.get_df_list(filename)
        return self.df_list

    def store_pickle(self,df,num):
        path = f'data/{self.department}/temp/df-{self.outname}-{num}.p'
        pickle_byte_obj = pickle.dumps(df) 
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=path, Body=pickle_byte_obj)
        # pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/df-{self.outname}-{num}.p", "wb" ))
    
    def load_pickle(self,num):
        path = f'data/{self.department}/temp/df-{self.outname}-{num}.p'
        my_pickle = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(path).get()['Body'].read())
        return my_pickle
        # return pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/df-{self.outname}-{num}.p", "rb" ))

    def save_csv(self,df):
        cleaned_file_path = f"data/{self.department}/temp/{self.outname}.csv"
        df.to_csv(f'{DirectoryFields.S3_PATH}{cleaned_file_path}', index=False, quoting=csv.QUOTE_ALL)
        # df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

if __name__ == "__main__":

    # s3 = boto3.resource('s3')
    # s3.Bucket('locus-data').put_object(Key='data/dca/temp/file.csv', Body="D")
    # my_df = pd.DataFrame(columns=["2","2"])
    # my_df.to_csv('s3://locus-data/data/dca/temp/file2.csv')
    # for bucket in s3.buckets.all():
    #     print(bucket.name)
    file_manager = FileManager('dca',['application','charge','inspection','license'],"application")

    