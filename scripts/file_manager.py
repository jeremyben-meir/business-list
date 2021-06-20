import pandas as pd
from datetime import date
from scripts.common import DirectoryFields
from os import listdir
from os.path import isfile, join
import pickle
import csv

class FileManager:

    def __init__(self, department, filenames, outname):
        self.filenames = filenames
        self.department = department
        self.df_list = []
        self.outname = outname

    def read_txt(self, path):
        textfile = open(path, "r")
        colnames = textfile.readline().strip("\n").split("\t")
        lines = textfile.readlines()
        listlist = [x.strip("\n").split("\t") for x in lines]
        return pd.DataFrame(listlist, columns=colnames)

    def get_df_list(self,filename):
        df_list = []
        path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/{filename}/"
        path_list = [f"{path}{f}" for f in listdir(path) if isfile(join(path, f)) and f[0] != '.']
        for path in path_list:
            if path[-4:] == '.txt':
                df_list.append(self.read_txt(path))
            else:
                df_list.append(pd.read_csv(path, low_memory=False))
        return df_list
    
    def check_relevance(self,filename):
        df = pd.read_csv(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/data-relevance.csv")
        df = df.set_index(['department','filename'])
        df['last_retrieved']=df['last_retrieved'].astype('datetime64[D]')
        df['api']=df['api'].astype(str)
        df['scrape_py']=df['scrape_py'].astype(str)
        df['FOIL']=df['FOIL'].astype(str)
        row = df.loc[(self.department,filename)]
        exp_date = row['last_retrieved'] + pd.Timedelta(days=row['retrieval_freq'])
        if exp_date <= date.today():
            if len(row['api']) > 0 and row['api'] != 'nan':
                print(f"Fetching from {row['api']}")
            elif len(row['scrape_py']) > 0 and row['scrape_py'] != 'nan':
                print(f"Running {row['FOIL']}")
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
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/df-{num}.p", "wb" ))
    
    def load_pickle(self,num):
        return pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/df-{num}.p", "rb" ))

    def save_csv(self,df):
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/{self.department}/temp/{self.outname}.csv"
        df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)