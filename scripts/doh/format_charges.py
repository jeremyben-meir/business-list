from global_vars import *

def begin_process():
  dh_file_path = LOCAL_LOCUS_PATH + "data/whole/DH/source/dh_10_20.txt"
  textfile = open(dh_file_path, "r")
  colnames = textfile.readline().strip("\n").split("\t")
  lines = textfile.readlines()

  listlist = [x.strip("\n").split("\t") for x in lines]
  df = pd.DataFrame(listlist, columns=colnames)

  pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/whole/DH/temp/dh-1.p", "wb" ))
  result_file_path = LOCAL_LOCUS_PATH + "data/whole/DH/dh_10_20.csv"
  df.to_csv(result_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

  print(len(df))

if __name__ == "__main__":
  begin_process()