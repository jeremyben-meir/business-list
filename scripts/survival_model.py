import boto3
import pickle
import pandas as pd
import sklearn.preprocessing
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import f1_score
from sklearn import svm, tree
import statsmodels.api as sm
from sklearn_pandas import DataFrameMapper
import numpy as np


class SurvivalModel():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        # NEED TO ELIMINATE 2021 OBS due to zeroed survival

    def generate_model(self):
        path = f'data/temp/df-observations.p'
        df = pickle.loads(self.s3.Bucket(
            "locus-data").Object(path).get()['Body'].read())
        df = df[df["Observed"] < pd.to_datetime(f"01/01/2020")]

        print(df.columns.tolist())

        starting_len = len(df)

        # FEATURES TO USE

        binar_list = ['zonedist1', 'bldgclass', 'histdist', 'landmark']
        int_list = ['lotarea', 'bldgarea', 'comarea', 'resarea', 'officearea', 'retailarea', 'garagearea', 'strgearea', 'factryarea', 'otherarea', 'numfloors', 'unitsres', 'unitstotal',
                    'lotfront', 'lotdepth', 'bldgfront', 'bldgdepth', 'bsmtcode', 'assessland', 'assesstot', 'yearbuilt', 'yearalter1', 'builtfar', 'residfar', 'commfar', 'facilfar',
                    "Months Active"]
        flt_list = ['GCP (NYC)', 'GDP (USA)', ' Payroll-Jobs Growth, SAAR - NYC', 'Payroll-Jobs Growth, SAAR - USA', 'PIT Withheld, Growth, NSA - NYC', 'PIT Withheld, Growth, NSA - USA',
                    'Inflation Rate, NSA - NYC', 'Inflation Rate, NSA - USA', 'Unemployment Rate, SA - NYC', 'Unemployment Rate, SA - USA']

        # DATA CLEANING BASED ON FEATURES

        for col in binar_list:
            df = df[~df[col].isna()]
            df[col] = df[col].apply(lambda val: val.strip())
            df[col] = df[col].astype(str)

        for col in int_list:
            df = df[~df[col].isna()]
            df[col] = df[col].astype(int)

        for col in flt_list:
            df = df[~df[col].isna()]
            df[col] = df[col].astype(float)

        print(df.head(100))
        print(f"DATA LOSS {starting_len} to {len(df)}")

        # FORMATTING ARRAY FOR MODELING

        mapper_list = [(col, sklearn.preprocessing.LabelBinarizer())
                       for col in binar_list] + [(col, None) for col in int_list] + [(col, None) for col in flt_list]
        mapper = DataFrameMapper(mapper_list)

        X = mapper.fit_transform(df.copy())
        Y = df['Survive'].to_numpy()

        ##########################################
        ##########################################

        X_train, X_test, Y_train, Y_test = train_test_split(
            X, Y, test_size=0.3, random_state=23)

        clf = MLPClassifier(solver='lbfgs', alpha=1e-5,
                            hidden_layer_sizes=(5, 2), random_state=1)
        clf.fit(X_train, Y_train)
        y_pred = clf.predict(X_test)
        print(f1_score(Y_test, y_pred, average='macro'))
        dat = {'y_test': Y_test,
               'y_pred': y_pred}
        results_df = pd.DataFrame(dat)
        print(results_df.head())
        print()

        ##########################################
        ##########################################

        clf2 = svm.SVC()
        clf2.fit(X_train, Y_train)
        y_pred2 = clf2.predict(X_test)
        print(f1_score(Y_test, y_pred2, average='macro'))
        print()

        ##########################################
        ##########################################

        clf3 = SGDClassifier(loss="hinge", penalty="l2", max_iter=30)
        clf3.fit(X_train, Y_train)
        y_pred3 = clf3.predict(X_test)
        print(f1_score(Y_test, y_pred3, average='macro'))
        print()

        ##########################################
        ##########################################

        clf4 = tree.DecisionTreeClassifier()
        clf4.fit(X_train, Y_train)
        y_pred4 = clf4.predict(X_test)
        print(f1_score(Y_test, y_pred4, average='macro'))
        print()

        # est2 = sm.Logit(Y, X).fit()
        # features = mapper.transformed_names_
        # feature_dict = {features[index]: (est2.params[index],est2.pvalues[index]) for index in range(len(features))}
        # feature_dict = {key:feature_dict[key][0] for key in feature_dict if feature_dict[key][1]<.05}
        # neighborhoods = dict(sorted(feature_dict.items(), key=lambda x: x[1], reverse=True)[:10])
        # print(neighborhoods)


if __name__ == "__main__":
    survival_model = SurvivalModel()
    survival_model.generate_model()
