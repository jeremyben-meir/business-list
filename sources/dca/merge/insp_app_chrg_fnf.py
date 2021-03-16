from global_vars import *

import multiprocessing
from multiprocessing import Process
from multiprocessing import Manager

def typecast(df):
  df["First Temp Op Letter Issued"] = pd.to_datetime(df["First Temp Op Letter Issued"]).dt.date
  df["Last Temp Op Letter Issued"] = pd.to_datetime(df["Last Temp Op Letter Issued"]).dt.date
  df["Last Temp Op Letter Expiration"] = pd.to_datetime(df["Last Temp Op Letter Expiration"]).dt.date
  df["Date of First Application"] = pd.to_datetime(df["Date of First Application"]).dt.date
  df["Date of Last Application"] = pd.to_datetime(df["Date of Last Application"]).dt.date
  df["Date of First Close"] = pd.to_datetime(df["Date of First Close"]).dt.date
  df["Date of Last Close"] = pd.to_datetime(df["Date of Last Close"]).dt.date
  df["Date of First OOB"] = pd.to_datetime(df["Date of First OOB"]).dt.date
  df["Date of Last OOB"] = pd.to_datetime(df["Date of Last OOB"]).dt.date
  df["Date of First Inspection"] = pd.to_datetime(df["Date of First Inspection"]).dt.date
  df["Date of Last Inspection"] = pd.to_datetime(df["Date of Last Inspection"]).dt.date
  df["Date of First Charge"] = pd.to_datetime(df["Date of First Charge"]).dt.date
  df["Date of Last Charge"] = pd.to_datetime(df["Date of Last Charge"]).dt.date
  df["End FNF Date"] = pd.to_datetime(df["End FNF Date"]).dt.date
  df["Start FNF Date"] = pd.to_datetime(df["Start FNF Date"]).dt.date

  return df

def get_bookend_date(row,max_true):
  #TODO: ADD RELEVANT DATE ROWS
  list_dates = ["First Temp Op Letter Issued","Last Temp Op Letter Issued","Date of First Application","Date of Last Application","Date of First Inspection","Date of Last Inspection","Date of First Charge","Date of Last Charge"]
  # print(row)
  # print(row[list_dates])
  # non_na_list = filter(lambda a: a != np.datetime64('NaT') and a != 'NaT', row[list_dates].tolist())
  non_na_list = [x for x in row[list_dates].tolist() if x != 'NaT' and x != np.datetime64('NaT') and not pd.isnull(x)]
  if len(non_na_list) != 0:
    return max(non_na_list) if max_true else min(non_na_list)
  return np.datetime64('NaT')


def compare_row_dates(fnf_row,dca_row, max_true):
  #TODO: FIGURE OUT FNF DATE COL NAME
  bookend_val = get_bookend_date(dca_row,max_true)
  if bookend_val != np.datetime64('NaT'):
    try:
      date_diff = (fnf_row["FEE DATE"] - bookend_val).days
    except:
      return (999999, dca_row.name) 
    # print(date_diff)
    if max_true:
      if date_diff < 0:
        return (999999, dca_row.name)
      return (date_diff,dca_row.name)
    return (abs(date_diff),dca_row.name)
  return (999999, dca_row.name) 

def add_fines_lines_helper(fnf_df,dca_df):
  residual_df = pd.DataFrame(columns=fnf_df.columns)
  progress = Progress_meter(limit = len(fnf_df.index))
  for index, row in fnf_df.iterrows():
    progress = progress.display()
    row_chosen = False
    dca_df_temp = dca_df.loc[dca_df.apply(lambda x: row["BUSINESS NAME"] in x["Business Name"] or row["BUSINESS NAME2"] in x["Business Name"], axis=1)]

    # print(dca_df.dtypes)
    if len(dca_df_temp.index) > 1:
      max_vals = min(dca_df_temp.apply(lambda dca_row: compare_row_dates(row,dca_row,max_true=True),axis=1), key = lambda t: t[0])
      min_vals = min(dca_df_temp.apply(lambda dca_row: compare_row_dates(row,dca_row,max_true=False),axis=1), key = lambda t: t[0])

      if not (max_vals[0] == 999999 or min_vals[0] == 999999 or max_vals[1] == min_vals[1]):
        cur_mt = dca_df.at[max_vals[1], "Moved To"]
        row_chosen = True
        if pd.isnull(cur_mt) or pd.isna(cur_mt) or cur_mt == "nan" or len(cur_mt)<5:##############
          dca_df.at[max_vals[1], "End FNF Date"] = row["FEE DATE"]
          dca_df.at[min_vals[1], "Start FNF Date"] = row["FEE DATE"]

          if dca_df.loc[max_vals[1], "LBID"] != dca_df.loc[min_vals[1], "LBID"]:
            dca_df = dca_df.replace(dca_df.loc[max_vals[1], "LBID"],dca_df.loc[min_vals[1], "LBID"])
            dca_df.at[max_vals[1], "Moved To"] = dca_df.loc[min_vals[1], "LLID"]
            dca_df.at[min_vals[1], "Moved From"] = dca_df.loc[max_vals[1], "LLID"]
          
        else:
          dca_df.at[max_vals[1], "End FNF Date"] = row["FEE DATE"]
          cur_llid = dca_df.at[max_vals[1], "LLID"]
          dca_df.loc[dca_df["Moved From"]==cur_llid, "Start FNF Date"] = row["FEE DATE"]
      elif max_vals[0] != 999999:
        row_chosen = True
        dca_df.at[max_vals[1], "End FNF Date"] = row["FEE DATE"]
      elif min_vals[0] != 999999:
        row_chosen = True
        dca_df.at[min_vals[1], "Start FNF Date"] = row["FEE DATE"]

    if not row_chosen:
      residual_df.loc[len(residual_df)] = row

  return dca_df,residual_df

def instatiate_variables():
  finished = False

  if not finished:

    fnf_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/DCA_Fines_and_Fees.csv"

    fnf_df = pd.read_csv(fnf_file_path)
    dca_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/df-6.p", "rb"))
    
    dca_df["End FNF Date"] = np.nan
    dca_df["Start FNF Date"] = np.nan
    dca_df = typecast(dca_df)
    fnf_df = fnf_df[fnf_df["RECORD TYPE"] == "Change of Address - Premise"]
    fnf_df["FEE DATE"] = pd.to_datetime(fnf_df["FEE DATE"]).dt.date

    result = add_fines_lines_helper(fnf_df,dca_df)
    dca_df = result[0]
    residual_df = result[1]

    pickle.dump(dca_df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/final-df.p", "wb"))
    pickle.dump(residual_df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/final-df-2.p", "wb"))

    finished == True


  updated = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/final-df.p", "rb"))
  updated.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/insp_app_chrg_fnf.csv",index=False,quoting=csv.QUOTE_NONNUMERIC)

  updated_r = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/final-df-2.p", "rb"))
  updated_r.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/fnf_residuals.csv",index=False,quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
  instatiate_variables()