from global_vars import *

def process_0():
  dh_file_path = LOCAL_LOCUS_PATH + "data/doh/dh_10_20.txt"
  textfile = open(dh_file_path, "r")
  colnames = textfile.readline().strip("\n").split("\t")
  lines = textfile.readlines()
  listlist = [x.strip("\n").split("\t") for x in lines]
  df = pd.DataFrame(listlist, columns=colnames)

  city_dict = {'Manhattan' : 1, 'Bronx': 2,'Brooklyn': 3, 'Queens': 4, 'Staten Island' : 5}
  
  pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doh/temp/df-dh.p", "wb" ))

if __name__ == "__main__":
  process_0()