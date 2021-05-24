from global_vars import *

def begin_process(segment):
  
  if 0 in segment:
    dh_file_path = LOCAL_LOCUS_PATH + "data/doh/dh_10_20.txt"
    textfile = open(dh_file_path, "r")
    colnames = textfile.readline().strip("\n").split("\t")
    lines = textfile.readlines()
    listlist = [x.strip("\n").split("\t") for x in lines]
    df = pd.DataFrame(listlist, columns=colnames)
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doh/temp/dh-1.p", "wb" ))

  if 1 in segment: 
    df = pickle.load( open(LOCAL_LOCUS_PATH + "data/doh/temp/dh-1.p", "rb" ))
    city_dict = {'Manhattan' : 1, 'Bronx': 2,'Brooklyn': 3, 'Queens': 4, 'Staten Island' : 5}
    
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doh/temp/dh-2.p", "wb" ))
    result_file_path = LOCAL_LOCUS_PATH + "data/doh/temp/dh_10_20.csv"
    df.to_csv(result_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
  begin_process([1])