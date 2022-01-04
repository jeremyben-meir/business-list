import boto3
import pickle
import pandas as pd
import sklearn.preprocessing
from scripts.prepare_geojson import PrepareGeojson
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score, precision_score, recall_score
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
from sksurv.metrics import cumulative_dynamic_auc
from sklearn.pipeline import make_pipeline
from sksurv.ensemble import RandomSurvivalForest
import matplotlib.pyplot as plt
from sklearn import tree

class SurvivalModel():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.sample_size = 50000
        # NEED TO ELIMINATE 2021 OBS due to zeroed survival

    def classifier_models(self):

        path = f'data/temp/df-observations.p'
        df = pickle.loads(self.s3.Bucket(
            "locus-data").Object(path).get()['Body'].read())
        print(df.columns.tolist())
        df_2021 = df[df["Year"] > 2020]
        df = pd.concat([df[df["Year"] < 2021].sample(self.sample_size),df_2021],ignore_index=True)
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
        feature_names = mapper.transformed_names_[1:]
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

        def decision_tree(clf):
            text_representation = tree.export_text(clf,max_depth=5, feature_names=feature_names)
            print(text_representation)

        def naive(zeros):
            if zeros:
                naive = np.zeros(shape=Y_test.shape)
            else:
                naive = np.ones(shape=Y_test.shape)            
            score = f1_score(Y_test, naive, average='macro')
            print(f"Naive {'zeros' if zeros else 'ones'}")
            print(f"F1 {score}")
            print(f"Accuracy {accuracy_score(Y_test, naive)}")
            print(f"ROC AUC {roc_auc_score(Y_test, naive)}")
            print(f"Precision {precision_score(Y_test, naive)}")
            print(f"Recall {recall_score(Y_test, naive)}")
            print()

        def try_clf(best_clf, clf, params):
            if params:
                clf = GridSearchCV(clf,params)
                clf.fit(X_train, Y_train)
                print(clf.best_params_)
            else:
                clf.fit(X_train, Y_train)
            y_pred = clf.predict(X_test)

            print()
            print()

            score = f1_score(Y_test, y_pred, average='macro')
            print(f"F1 {score}")
            print(f"Accuracy {accuracy_score(Y_test, y_pred)}")
            print(f"ROC AUC {roc_auc_score(Y_test, y_pred)}")
            print(f"Precision {precision_score(Y_test, y_pred)}")
            print(f"Recall {recall_score(Y_test, y_pred)}")
            print()
            # decision_tree(clf)
            if best_clf["f1"] < score:
                best_clf["clf"] = clf
                best_clf["f1"] = score
            return best_clf
        
        clf_list = [
            (MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(5, 2), random_state=1, max_iter=400), {}),
            (svm.SVC(), {}),
            (SGDClassifier(loss="hinge", penalty="l2", max_iter=100), {}),
            (tree.DecisionTreeClassifier(), {"criterion":['gini', 'entropy'],"splitter":['best', 'random']}),
            (tree.DecisionTreeClassifier(criterion='gini',splitter='best'), {}),
            (RandomForestClassifier(), {"criterion":['gini', 'entropy'],"max_depth":range(10,20)})
        ]

        for clf , params in clf_list:
            print(clf)
            best_clf = try_clf(best_clf,clf,params)
        
        naive(True)
        naive(False)
        
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
        collection = PrepareGeojson.create_llid_json(df_2021,"Prediction")
        self.s3.Bucket(DirectoryFields.S3_PATH_NAME).put_object(Key="data/geo/predictions.json", Body=('%s' % collection))
    
    def survival_models(self):
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

        va_x = df[binar_list + int_list + flt_list]
        va_y = Surv.from_dataframe("Status","Months Active",df)

        va_x_train, va_x_test, va_y_train, va_y_test = train_test_split(
            va_x, va_y, test_size=0.2, stratify=va_y["Status"], random_state=0
        )
        
        #################################################

        # cph = make_pipeline(OneHotEncoder(), CoxPHSurvivalAnalysis(alpha = 1e-4))
        # cph.fit(va_x_train, va_y_train)
        # print(cph.named_steps["coxphsurvivalanalysis"].coef_)

        encoder = OneHotEncoder().fit(va_x)
        df_train_num = encoder.transform(va_x_train)
        df_test_num = encoder.transform(va_x_test)
        estimator = CoxPHSurvivalAnalysis(alpha = 1e-4)
        estimator.fit(df_train_num, va_y_train)

        pd.set_option('display.max_rows', None)
        print(pd.Series(estimator.coef_, index=df_train_num.columns))

        va_times = np.arange(1, 32)
        cph_risk_scores = estimator.predict(df_test_num)
        cph_auc, cph_mean_auc = cumulative_dynamic_auc(
            va_y_train, va_y_test, cph_risk_scores, va_times
        )

        print(estimator.score(df_test_num, va_y_test))

        pred_surv = estimator.predict_survival_function(df_train_num)
        time_points = np.arange(1, 120)
        for i, surv_func in enumerate(pred_surv):
            plt.step(time_points, surv_func(time_points), where="post",
                    label="Sample %d" % (i + 1))
        plt.ylabel("est. probability of survival $\hat{S}(t)$")
        plt.xlabel("time $t$")
        plt.legend(loc="best")
        plt.show()

        print(cph_auc)
        print(cph_mean_auc)

        #################################################

        # va_times = np.arange(1, 32)

        # rsf = make_pipeline(
        #     OneHotEncoder(),
        #     RandomSurvivalForest(n_estimators=100, min_samples_leaf=7, random_state=0)
        # )
        # rsf.fit(va_x_train, va_y_train)

        # rsf_chf_funcs = rsf.predict_cumulative_hazard_function(
        #     va_x_test, return_array=False)
        # rsf_risk_scores = np.row_stack([chf(va_times) for chf in rsf_chf_funcs])

        # rsf_auc, rsf_mean_auc = cumulative_dynamic_auc(
        #     va_y_train, va_y_test, rsf_risk_scores, va_times
        # )

        # print(rsf_auc)
        # print(rsf_mean_auc)

        #################################################

if __name__ == "__main__":
    survival_model = SurvivalModel()
    survival_model.classifier_models()
    # survival_model.survival_models()

# # df_num = encoder.transform(df)
# # poly = PolynomialFeatures(interaction_only=True,include_bias = False).fit(df_num)
# # df_train_num = poly.transform(df_train_num)
# # df_test_num = poly.transform(df_test_num)