from global_vars import *

def begin_process():
    revocation_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/License_Revocations.csv"
    my_df = pd.read_csv(revocation_file_path)

    my_df["Event Date"] = my_df["Event Date"].astype('datetime64[D]')
    my_df["Event Type"] = my_df["Event Type"].astype(str)

    my_df = my_df.loc[my_df.reset_index().groupby(['DCA License Number'])['Event Date'].idxmax()]
    my_df = my_df[~my_df["Event Type"].str.contains("Reinstated", na=False)]

    print(my_df)

    my_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/revocations_updated_0.csv",index=False,quoting=csv.QUOTE_ALL)

if __name__ == "__main__":
    begin_process()