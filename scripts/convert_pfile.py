import pickle
from common import DirectoryFields
import boto3
import csv

class ConvertPFile():

    def __init__(self):
        self.s3 = boto3.resource('s3')

    def convertFile(self):
        get_path = input("Get path (without .p extension): ") #data/temp/df-observations
        put_path = input(f"Put path [{get_path}.csv]: ")
        sample_size = input(f"Sample Size [Entire dataframe]: ")
        sample_size = int(sample_size) if sample_size != "" else 0
        put_path = get_path if put_path == "" else put_path

        self.df = pickle.loads(self.s3.Bucket(DirectoryFields.S3_PATH_NAME).Object(get_path+".p").get()['Body'].read())
        
        full_put_path = f"{DirectoryFields.S3_PATH}"+get_path+".csv"

        if sample_size > 0:
            self.df = self.df.sample(sample_size)
        self.df.to_csv(full_put_path, index=False, quoting=csv.QUOTE_ALL)
    
    def get_file_data(self):
        print(f"Length: {len(self.df)}")
        print(self.df)

if __name__ == "__main__":
    converter = ConvertPFile()
    converter.convertFile()
    converter.get_file_data()
