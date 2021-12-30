import boto3
import pickle
import pandas as pd
import sklearn.preprocessing
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score
from sklearn import svm, tree
import statsmodels.api as sm
from sklearn_pandas import DataFrameMapper
from sklearn.impute import SimpleImputer
import numpy as np
from scripts.common import DirectoryFields
import csv
from collections import defaultdict
from sklearn import preprocessing
import math
from geojson import Feature, FeatureCollection, Point
from sklearn.ensemble import RandomForestClassifier
from hmmlearn import hmm
from sklearn.model_selection import ParameterGrid,GridSearchCV
from sksurv.linear_model import CoxPHSurvivalAnalysis
from sksurv.preprocessing import OneHotEncoder
from sksurv.util import Surv
from sklearn.preprocessing import PolynomialFeatures

class SurvivalModel():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.sample_size = 50000
        # NEED TO ELIMINATE 2021 OBS due to zeroed survival

    def generate_model(self):

        path = f'data/temp/df-observations.p'
        df = pickle.loads(self.s3.Bucket(
            "locus-data").Object(path).get()['Body'].read())
        print(df.columns.tolist())
        df_2021 = df[df["Observed"] > pd.to_datetime(f"01/01/2020")]
        df = pd.concat([df[df["Observed"] < pd.to_datetime(f"01/01/2021")].sample(self.sample_size),df_2021],ignore_index=True)
        starting_len = len(df)

        # FEATURES TO USE

        id_list = ['Year']
        binar_list = ['zonedist1', 'bldgclass', 'histdist', 'landmark']
        int_list = ['lotarea', 'bldgarea', 'comarea', 'resarea', 'officearea', 'retailarea', 'garagearea', 'strgearea', 'factryarea', 'otherarea', 'numfloors', 'unitsres', 'unitstotal',
                    'lotfront', 'lotdepth', 'bldgfront', 'bldgdepth', 'bsmtcode', 'assessland', 'assesstot', 'yearbuilt', 'yearalter1', 'builtfar', 'residfar', 'commfar', 'facilfar',
                    "Months Active"]
        flt_list = ['GCP (NYC)', 'GDP (USA)', ' Payroll-Jobs Growth, SAAR - NYC', 'Payroll-Jobs Growth, SAAR - USA', 'PIT Withheld, Growth, NSA - NYC', 'PIT Withheld, Growth, NSA - USA',
                    'Inflation Rate, NSA - NYC', 'Inflation Rate, NSA - USA', 'Unemployment Rate, SA - NYC', 'Unemployment Rate, SA - USA']

        # DATA CLEANING BASED ON FEATURES

        def fix_df(to_df, dropna = False):
            if dropna:
                for col in binar_list:
                    to_df = to_df[~to_df[col].isna()]
                    to_df[col] = to_df[col].apply(lambda val: val.strip())
                    to_df[col] = to_df[col].astype(str)

                for col in int_list:
                    to_df = to_df[~to_df[col].isna()]
                    to_df[col] = to_df[col].astype(int)

                for col in flt_list:
                    to_df = to_df[~to_df[col].isna()]
                    to_df[col] = to_df[col].astype(float)
                
                print(f"DATA LOSS {starting_len} to {len(df)}")

            else:
                for col in binar_list+flt_list+int_list:
                    to_df[col].fillna(to_df[col].mode()[0], inplace=True)

            return to_df

        df = fix_df(df)

        mapper_list = [(col, None) for col in id_list] + [(col, sklearn.preprocessing.LabelBinarizer())
                       for col in binar_list] + [(col, None) for col in int_list] + [(col, None) for col in flt_list]
        mapper = DataFrameMapper(mapper_list)

        X = mapper.fit_transform(df.copy())
        Y = df['Survive'].to_numpy()

        X_2021 = X[self.sample_size:,1:]
        Y_2021 = Y[self.sample_size:]
        X = X[:self.sample_size,1:]
        Y = Y[:self.sample_size]

        # print(X)
        # print(X_2021)
        print()

        X_train, X_test, Y_train, Y_test = train_test_split(
            X, Y, test_size=0.3, random_state=23)

        best_clf = {"clf":None,"f1":0}
        def try_clf(best_clf, clf, params):
            if params:
                clf = GridSearchCV(clf,params)
                clf.fit(X_train, Y_train)
                print(clf.best_params_)
            else:
                clf.fit(X_train, Y_train)
            y_pred = clf.predict(X_test)
            score = f1_score(Y_test, y_pred, average='macro')
            print(score)
            print(accuracy_score(Y_test, y_pred))
            print(roc_auc_score(Y_test, y_pred))
            print()
            if best_clf["f1"] < score:
                best_clf["clf"] = clf
                best_clf["f1"] = score
            return best_clf
        
        clf_list = [
            # (MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(5, 2), random_state=1, max_iter=400), {}),
            # (svm.SVC(), {}),
            # (SGDClassifier(loss="hinge", penalty="l2", max_iter=100), {}),
            (tree.DecisionTreeClassifier(), {"criterion":['gini', 'entropy'],"splitter":['best', 'random']}),
            # (RandomForestClassifier(), {"criterion":['gini', 'entropy'],"max_depth":range(10,20)})
        ]

        for clf , params in clf_list:
            print(clf)
            best_clf = try_clf(best_clf,clf,params)
        
        # clf , params = clf_list[0]
        # best_clf = try_clf(best_clf,clf,params)
        # clf , params = clf_list[1]
        # best_clf = try_clf(best_clf,clf,params)
        # clf , params = clf_list[2]
        # best_clf = try_clf(best_clf,clf,params)
        # clf , params = clf_list[3]
        # best_clf = try_clf(best_clf,clf,params)
        # clf , params = clf_list[4]
        # best_clf = try_clf(best_clf,clf,params)

        Y_pred = best_clf["clf"].predict(X_2021)
        df_2021["Survive"] = Y_pred
        self.survive_geojson(df_2021)
    
    def cox_hazards(self):
        path = f'data/temp/df-observations-cox.p'
        df = pickle.loads(self.s3.Bucket(
            "locus-data").Object(path).get()['Body'].read())
        df = df.sample(130000).reset_index(drop=True)
        print(df.columns.tolist())

        binar_list = ['zonedist1', 'bldgclass']
        int_list = ['lotarea','garagearea', 'strgearea', 'numfloors', 'unitstotal', 'bldgdepth', 'yearbuilt', 'builtfar']
        flt_list = ['GCP (NYC)', ' Payroll-Jobs Growth, SAAR - NYC', 'Inflation Rate, NSA - NYC',  'Unemployment Rate, SA - NYC']

        for col in binar_list:
            df[col] = df[col].astype("category")
            df[col].fillna(df[col].mode()[0], inplace=True)
        for col in int_list+flt_list:
            df[col] = df[col].astype(float)
            df[col].fillna(df[col].mode()[0], inplace=True)

        df_train = df.iloc[:100000]
        df_test = df.iloc[100000:]

        df_train_y = Surv.from_dataframe("Status","Months Active",df_train)
        df_test_y = Surv.from_dataframe("Status","Months Active",df_test)

        df = df[binar_list + int_list + flt_list]

        encoder = OneHotEncoder().fit(df)
        df_train_num = encoder.transform(df_train)
        df_test_num = encoder.transform(df_test)

        # df_num = encoder.transform(df)
        # poly = PolynomialFeatures(interaction_only=True,include_bias = False).fit(df_num)
        # df_train_num = poly.transform(df_train_num)
        # df_test_num = poly.transform(df_test_num)

        # print(df_train_num)
        # print(df_test_num)

        estimator = CoxPHSurvivalAnalysis(alpha = 1e-4)
        estimator.fit(df_train_num, df_train_y)
        pd.set_option('display.max_rows', None)
        print(pd.Series(estimator.coef_, index=df_train_num.columns))

        print(estimator.score(df_test_num, df_test_y))

    def survive_geojson(self,df): ## ADAPT IN PREPARE GEOJSON @staticmethod
        features = list()
        print(len(df))
        totlen = df["BBL"].nunique()
        ticker = 0
        grouped = df.groupby("BBL")
        for name, group in grouped:
            num_vals = len(group)
            lon = group["Longitude"].astype(float).max()
            lat = group["Latitude"].astype(float).max()
            counter = 0
            for index, row in group.iterrows():
                duration = (row["End Date"] - row["Start Date"]).days
                duration =  str(math.floor(duration/365.0) if not pd.isnull(duration) else 0)
                
                if num_vals == 1:
                    latitude, longitude = map(float, (lat, lon))
                else:
                    degree_val = 360.0*(counter/num_vals)
                    rand_radian = math.radians(degree_val)
                    latitude, longitude = map(float, (lat+(math.sin(rand_radian)/15000.0), lon+(math.cos(rand_radian)/12000.0)))
                
                counter += 1

                features.append(
                    Feature(
                        geometry = Point((longitude, latitude)),
                        properties = {
                            'Name': row["Name"],
                            'LLID': row["LLID"],
                            'Address': row["Address"],
                            # 'NAICS': str(row["NAICS"])[0],
                            # 'NAICS Title': str(row["NAICS Title"]),
                            'Contact Phone': str(row["Contact Phone"]),
                            "Duration": duration,
                            'Start Date': str(row["Start Date"]),
                            'End Date': str(row["End Date"]),
                            "color": str(1-row["Survive"]),
                        }
                    )
                )
            ticker += 1
            print(f" {ticker} / {totlen}",end="\r")
        print("\nPredictions complete")
        collection = FeatureCollection(features)
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="data/geo/predictions.json", Body=('%s' % collection))

if __name__ == "__main__":
    survival_model = SurvivalModel()
    survival_model.generate_model()
    # survival_model.cox_hazards()


# est2 = sm.Logit(Y, X).fit()
# features = mapper.transformed_names_
# feature_dict = {features[index]: (est2.params[index],est2.pvalues[index]) for index in range(len(features))}
# feature_dict = {key:feature_dict[key][0] for key in feature_dict if feature_dict[key][1]<.05}
# neighborhoods = dict(sorted(feature_dict.items(), key=lambda x: x[1], reverse=True)[:10])
# print(neighborhoods)

    
# DF 2021 columns nununique all equal 1, so binarizer  doesnt asss cols

# print(len(binar_list+flt_list+int_list))
# for col in binar_list+flt_list+int_list:
#     df_2021[col].fillna(df[col].mode()[0], inplace=True)
#     print(df_2021[col])
#     print(df_2021[col].nunique())
# df_2021 = fix_df(df_2021)
# mapper_list = [(col, sklearn.preprocessing.LabelBinarizer()) for col in binar_list] + [(col, None) for col in int_list] + [(col, None) for col in flt_list]
# mapper = DataFrameMapper(mapper_list)
# X_2021 = mapper.fit_transform(df_2021.copy())