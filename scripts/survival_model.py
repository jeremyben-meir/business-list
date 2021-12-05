import boto3
import pickle
import pandas as pd
import sklearn.preprocessing
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import f1_score
import statsmodels.api as sm
from sklearn_pandas import DataFrameMapper
import numpy as np
import names

class SurvivalModel():
    
    def __init__(self):
        self.s3 = boto3.resource('s3')
        # NEED TO ELIMINATE 2021 OBS due to zeroed survival
        # df = df[df["Observed"] < pd.to_datetime(f"01/01/2021")]

    def generate_model(self):
        path = f'data/temp/df-observations.p'
        business_dataloc_list = pickle.loads(self.s3.Bucket("locus-data").Object(path).get()['Body'].read())

        print(business_dataloc_list.columns.tolist())

        starting_len = len(business_dataloc_list)

        # FEATURES TO USE

        binar_list = ['zonedist1', 'bldgclass','histdist','landmark']
        num_list =  ['lotarea', 'bldgarea', 'comarea', 'resarea', 'officearea', 'retailarea', 'garagearea', 'strgearea', 'factryarea', 'otherarea', 'numfloors', 'unitsres', 'unitstotal', 'lotfront', 'lotdepth', 'bldgfront', 'bldgdepth', 'bsmtcode', 'assessland', 'assesstot', 'yearbuilt', 'yearalter1', 'builtfar', 'residfar', 'commfar', 'facilfar'] 
        
        # DATA CLEANING BASED ON FEATURES

        for col in binar_list:
            business_dataloc_list = business_dataloc_list[~business_dataloc_list[col].isna()]
            business_dataloc_list[col] = business_dataloc_list[col].apply(lambda val: val.strip())
            business_dataloc_list[col] = business_dataloc_list[col].astype(str)

        for col in num_list:
            business_dataloc_list = business_dataloc_list[~business_dataloc_list[col].isna()]
            business_dataloc_list[col] = business_dataloc_list[col].astype(int)
    
        print(f"DATA LOSS {starting_len} to {len(business_dataloc_list)}")

        # FORMATTING ARRAY FOR MODELING

        mapper_list = [(col,sklearn.preprocessing.LabelBinarizer()) for col in binar_list] + [(col,None) for col in num_list]
        mapper = DataFrameMapper(mapper_list)

        X = mapper.fit_transform(business_dataloc_list.copy())
        Y = business_dataloc_list['Survive'].to_numpy()

        ##########################################
        ##########################################

        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3, random_state=23)

        clf = MLPClassifier(solver='lbfgs', alpha=1e-5,hidden_layer_sizes=(5, 2), random_state=1)
        clf.fit(X_train, Y_train)
        y_pred = clf.predict(X_test)
        print(f1_score(Y_test, y_pred, average='macro'))
        print()

        ##########################################
        ##########################################

        # est2 = sm.Logit(Y, X).fit()

        # features = mapper.transformed_names_
        # feature_dict = {features[index]: (est2.params[index],est2.pvalues[index]) for index in range(len(features))}
        # feature_dict = {key:feature_dict[key][0] for key in feature_dict if feature_dict[key][1]<.05}
        # neighborhoods = dict(sorted(feature_dict.items(), key=lambda x: x[1], reverse=True)[:10])
        # print(neighborhoods)
            

if __name__ == "__main__":
    survival_model = SurvivalModel()
    survival_model.generate_model()