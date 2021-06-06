from global_vars import *

def process_0():
  dh_file_path = LOCAL_LOCUS_PATH + "data/doh/doh_10-20.txt"
  textfile = open(dh_file_path, "r")
  colnames = textfile.readline().strip("\n").split("\t")
  lines = textfile.readlines()
  listlist = [x.strip("\n").split("\t") for x in lines]
  df = pd.DataFrame(listlist, columns=colnames)
  pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh.p", "wb" ))

def process_1():
  
  df = pickle.load( open(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh.p", "rb" ))
 
  df['BORO'] = df['BORO'].replace(['0','1','2','3',"4","5"],['','NEW YORK', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND'])
  df['Building Number'] = df['ADDRESS'].apply(lambda x: x.split(" ")[0])
  df['Street'] = df['ADDRESS'].apply(lambda x: " ".join(x.split(" ")[1:]))
  df['CUISINECODE'] = df['CUISINECODE'].apply(lambda x: "Restaurant-"+x)
  df['CUISINECODE'] = df['CUISINECODE'].apply(lambda x: x.strip('        '))
  df['State'] = 'NY'

  del df['ADDRESS']
  del df['ACTION']
  del df['PROGRAM']
  del df['INSPTYPE']
  del df['VIOLCODE']
  del df['DISMISSED']
  del df['SCORE']
  del df['VIOLSCORE']
  del df['MOD_TOTALSCORE']

  df = df.rename(columns={"CAMIS": "Record ID", 'DBA':'Business Name', 'BORO':'City', 'ZIPCODE':'Zip','PHONE':'Contact Phone', 'CUISINECODE':'Industry','INSPDATE':'INSP Date','CASE_DECISION_DATE':'Case Dec. Date','CURRENTGRADE':'Current Grade','GRADEDATE':'Current Grade Date'})
  
  df['Record ID'] = df['Record ID'].astype(str)
  df['Business Name'] = df['Business Name'].astype(str)
  df['Building Number'] = df['Building Number'].astype(str)
  df['Street'] = df['Street'].astype(str)
  df["City"] = df["City"].astype(str)
  df['Zip'] = df['Zip'].astype(str)
  df['Contact Phone'] = df['Contact Phone'].astype(str)
  df['Industry'] = df['Industry'].astype(str)
  df['INSP Date'] = df['INSP Date'].astype('datetime64[D]')
  df['Case Dec. Date'] = df['Case Dec. Date'].replace(['N/A','NULL','nan'],'')
  df['Case Dec. Date'] = df['Case Dec. Date'].astype('datetime64[D]')
  df['Current Grade'] = df['Current Grade'].astype(str)
  df['Current Grade Date'] = df['Current Grade Date'].replace(['N/A','NULL','nan'],'')
  df['Current Grade Date'] = df['Current Grade Date'].astype('datetime64[D]')

  global_counter_init(len(df))
  df = clean_zip_city(df)

  df['BBL'] = ''
  df = df.drop_duplicates()
 
  pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh-1.p", "wb" ))
  
def process_2():
  df = pickle.load( open(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh-1.p", "rb" ))
  global_counter_init(len(df))
  df = df.apply(lambda row: add_bbl(row, overwrite=False), axis=1)
  pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh-2.p", "wb" ))

def process_3():
  df = pickle.load( open(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh-2.p", "rb" ))
  df.to_csv(LOCAL_LOCUS_PATH + "data/doh/temp/df-doh.csv") 

if __name__ == "__main__":
  process_2()