from global_vars import *

def start_merge():
    liquors_base_path = LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/liquors_base.csv"

    base_df = pd.read_csv(liquors_base_path)
    end_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/temp/file-fin-2.p", "rb"))

    print(len(base_df.index))
    print(len(end_df.index))

    base_df["Serial"] = base_df["Serial"].astype(str)
    end_df["Serial"] = end_df["Serial"].astype(str)

    new_df = pd.merge(base_df, end_df, how='outer',left_on=["Serial"],right_on=["Serial"])

    new_df["Premises Name"]=new_df["Premises Name_x"]
    del new_df["Premises Name_x"]
    del new_df["Premises Name_y"]

    # print(len(new_df.index))
    
    # base_df = base_df[(~base_df["Serial"].isin(new_df["Serial"]))].drop_duplicates(["Premises Name","Class"])
    # end_df = end_df[(~end_df["Serial"].isin(new_df["Serial"]))].drop_duplicates(["Premises Name","License Class"])

    # print(len(base_df.index))
    # print(len(end_df.index))
    
    # new_df_2 = pd.merge(base_df, end_df, how='outer',left_on=["Premises Name","Class"],right_on=["Premises Name","License Class"])

    # print(len(new_df_2.index))

    # new_df_2["Serial"]=new_df_2["Serial_x"]
    # del new_df_2["Serial_x"]
    # del new_df_2["Serial_y"]

    # base_df = base_df[(~base_df["Serial"].isin(new_df_2["Serial"]))]
    # end_df = end_df[(~end_df["Serial"].isin(new_df_2["Serial"]))]

    # new_df_3 = pd.merge(base_df, end_df, how='outer',left_on=["Premises Name"],right_on=["Premises Name"])

    # final_df = pd.concat([new_df,new_df_2])

    new_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/liquor_licenses/liquors_merged.csv",index=False,quoting=csv.QUOTE_ALL)


if __name__ == '__main__':
    start_merge()