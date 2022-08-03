import pickle
from common import DirectoryFields
import boto3
import csv

class CreateVerifiable():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        
    def get_pluto(self,year):

        def set_str(bbl):
            try:
                return str(int(bbl))
            except Exception:
                return None

        path = f'pluto/{year}.p'
        pluto_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        pluto_df["bbl"] = pluto_df["bbl"].apply(lambda bbl: set_str(bbl))
        pluto_df = pluto_df[["bbl","comarea","unitstotal","unitsres"]]
        return pluto_df

    def convertFile(self):
        get_path = "data/temp/df-timeline"
        pluto_df = self.get_pluto(2021)
        year_list_str = [str(x) for x in list(range(2010,2022))]

        print(pluto_df)

        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(get_path+".p").get()['Body'].read())

        self.df = self.df.merge(pluto_df, how='left', left_on=['BBL'], right_on=['bbl'])
        self.df = self.df.loc[:,~self.df.columns.duplicated()]
        self.df = self.df.loc[~self.df.index.duplicated(keep='first')]
        self.df = self.df.reset_index(drop = True)

        self.df["OSD"] = 1
        self.df["Match?"] = ""
        for col in year_list_str:
            self.df[col] = ""
        self.df["LatLon"] = self.df.apply(lambda row: str(row["Latitude"]) + " " + str(row["Longitude"]),axis=1)

        self.df["comarea"] = self.df["comarea"].fillna(-1).astype(int)
        self.df["unitstotal"] = self.df["unitstotal"].fillna(-1).astype(int)
        self.df["unitsres"] = self.df["unitsres"].fillna(-1).astype(int)
        bad_lbid_mask = (((self.df["unitstotal"] == self.df["unitsres"]) & (self.df["unitstotal"] != -1) & (self.df["unitsres"] != -1)) | (self.df["comarea"] == 0))
        self.df = self.df[~bad_lbid_mask]

        unique_bbl_sample = self.df.sample(n=200)["BBL"].unique()
        self.df = self.df[self.df["BBL"].isin(unique_bbl_sample)]

        self.df = self.df[['Name', 'Name 1', 'Name 2', 'Name 3', 'OSD', 'Match?', 'BBL', 'Address', 'LatLon', 'Start Date', 'End Date'] + year_list_str]
        self.df.to_csv(f"{DirectoryFields.S3_PATH}"+get_path+".csv", index=False, quoting=csv.QUOTE_ALL)

if __name__ == "__main__":
    creator = CreateVerifiable()
    creator.convertFile()
