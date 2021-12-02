from datetime import date
import pickle

from pandas.tseries.offsets import YearBegin
from common import DirectoryFields
import pandas as pd
import csv
import boto3

class DateLocObservations():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        # self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object("data/temp/df-timeline.p").get()['Body'].read())

    def generate(self):
        # self.df["Observed"] = pd.NaT
        pass

if __name__ == "__main__":
    date_loc_observations = DateLocObservations()
    date_loc_observations.generate()