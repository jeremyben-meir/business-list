import boto3
import pickle
import pandas as pd
import names

class MergeObservations():
    
    def __init__(self):
        self.s3 = boto3.resource('s3')

    def get_pluto(self,year):
        path = f'pluto/{year}/pluto{year}.p'
        pluto_df = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        return pluto_df

    def get_business_obs(self):
        path = f'data/temp/df-business-observations.p'
        business_list = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())
        return business_list
    
    def generate_data(self):
        pluto_dict = {year: self.get_pluto(year) for year in range(2010,2021)}
        business_df = self.get_business_obs()
        business_df_list = list()

        for year in pluto_dict:
            pluto_dict[year]["bbl"] = pluto_dict[year]["bbl"].astype(str)
            business_df_year = business_df[business_df["Year"] == year]
            merged = business_df_year.merge(pluto_dict[year], how='left', left_on=['BBL'], right_on=['bbl'])
            merged = merged.loc[:,~merged.columns.duplicated()]
            business_df_list.append(merged)
            
        for idx in range(len(business_df_list)):
            business_df_list[idx] = business_df_list[idx].loc[~business_df_list[idx].index.duplicated(keep='first')]
            business_df_list[idx] = business_df_list[idx].reset_index(drop = True)

        total_df = pd.concat(business_df_list, ignore_index=True)

        total_df = total_df[~(total_df["unitstotal"] == total_df["unitsres"])]
        total_df = total_df[~(total_df["comarea"] == 0)]

        total_df = total_df.reset_index(drop=True)
        self.s3.Bucket("locus-data").put_object(Key='data/temp/df-observations.p', Body=pickle.dumps(total_df))

if __name__ == "__main__":
    merge_observations = MergeObservations()
    merge_observations.generate_data()