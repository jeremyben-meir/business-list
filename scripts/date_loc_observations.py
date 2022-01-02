from datetime import date
import pickle
from scripts.file_manager import FileManager
from pandas.tseries.offsets import YearBegin
from common import DirectoryFields
import pandas as pd
import csv
import boto3
from decimal import Decimal
from geosupport import Geosupport

class DateLocObservations():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.g = Geosupport()
        self.counter = 0
    
    def apply_trans(self, res_df, year):
        res_df["Year"] = year
        res_df = res_df.reset_index(drop=True)
        res_df.columns = res_df.columns.str.lower()
        return res_df
    
    def add_bbl_features(self,bbl,lendf):
        self.counter += 1
        print(f"{self.counter} / {lendf}")
        try:
            result = self.g.bbl({"bbl":str(int(bbl))})
            lon = float(result["Longitude"])
            lat = float(result["Latitude"])
            subway = len(self.subway_df[(abs(self.subway_df["longitude"]-lon)<.002) & (abs(self.subway_df["latitude"]-lat)<.002)])
            return subway
        except Exception as e:
            return None

    def generate_pluto(self):

        self.subway_df = self.generate_subway()

        for year in range(2010,2022):
            path = f"pluto/source/{year}/"
            df_list = list()
            summary_list = [obj.key for obj in self.s3.Bucket(DirectoryFields.S3_PATH_NAME).objects.filter(Prefix=path) if (obj.key != path and "/." not in obj.key and (".csv" in obj.key or ".txt" in obj.key))]
            if len(summary_list) > 0:
                if len(summary_list) > 1:
                    for key_val in summary_list:
                        df = pd.read_csv(f"{DirectoryFields.S3_PATH}{key_val}", sep=",",low_memory=False)
                        df_list.append(df)
                    res_df = pd.concat(df_list)
                else:
                    res_df = pd.read_csv(f"{DirectoryFields.S3_PATH}{summary_list[0]}", sep=",",low_memory=False)
                res_df = self.apply_trans(res_df, year)
                print(year)
                print(res_df.columns.tolist())
                # lendf = len(res_df)                    
                # res_df["Subway"] = res_df["bbl"].apply(lambda bbl: self.add_bbl_features(bbl,lendf))
                self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"pluto/{year}.p", Body=pickle.dumps(res_df))

    def generate_comptroller(self):
        path = f"comptroller/source/key_indicators.csv"
        df = pd.read_csv(f"{DirectoryFields.S3_PATH}{path}", sep=",",low_memory=False)
        df.rename( columns={'Unnamed: 0':'year'}, inplace=True )
        df = df[pd.to_datetime(df['year']).dt.month == 1]
        df['year'] = pd.to_datetime(df['year']).dt.year
        df = df.reset_index(drop=True)
        df = df.applymap(lambda cell: float(cell[:-1])/100.0 if type(cell) == str and cell[-1] == "%" else cell)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"comptroller/key_indicators.p", Body=pickle.dumps(df))
    
    def generate_subway(self):
        path = f"subway/source/DOITT_SUBWAY_STATION_01_13SEPT2010.csv"
        df = pd.read_csv(f"{DirectoryFields.S3_PATH}{path}", sep=",",low_memory=False)
        df = df.reset_index(drop=True)
        df["longitude"] = df["the_geom"].apply(lambda cell: float(cell.split(" ")[1][1:]))
        df["latitude"] = df["the_geom"].apply(lambda cell: float(cell.split(" ")[2][:-1]))
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key=f"subway/data.p", Body=pickle.dumps(df))
        return df

if __name__ == "__main__":
    date_loc_observations = DateLocObservations()
    date_loc_observations.generate_pluto()
    # date_loc_observations.generate_comptroller()

# def min_dist(row,lon,lat,subway_df):
#     # min_val = min(subway_df.apply(lambda row: math.sqrt(((row["longitude"]-lon)**2)+((row["latitude"]-lat)**2)),axis=1))
#     min_val = len(subway_df[(abs(subway_df["longitude"]-lon)<.005) &(abs(subway_df["latitude"]-lat)<.005)])
#     row["subway"] = min_val
#     return row
# res_df = res_df.apply(lambda row: min_dist(row,row["longitude"],row["latitude"],subway_df),axis=1)
