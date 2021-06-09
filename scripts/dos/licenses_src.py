from global_vars import *

def instantiate_file():
  # Get file paths
  aes_file_path = LOCAL_LOCUS_PATH + "data/dos/aes_10-20.csv"
  barber_file_path = LOCAL_LOCUS_PATH + "data/dos/barber_92-20.csv"
  # Get dfs from file paths
  df_aes = pd.read_csv(aes_file_path)
  df_barber = pd.read_csv(barber_file_path)
  # Concatenate the dfs  
  df_aes['Industry'] = 'Appearance Enhancement Service'
  df_barber['Industry'] = 'Barber'
  df = pd.concat([df_aes,df_barber], axis=0, join='outer', ignore_index=False)

  df = df.rename(columns={"License Number": "Record ID", 'Name':'Business Name 2', 'County':'City', 'License State':'State' ,'Phone':'Contact Phone', 'License Issue Date':'LIC Issue Date', 'Current Term Effective Date':'LIC Current Issue Date', 'Expiration Date':'LIC Exp Date', 'License Status':'LIC Status'})
 
  df['Address'] = df['Address'].apply(lambda x: str(x).replace('                ',' '))

#   df['Building Number'] = df['STREET'].apply(lambda x: x.split(" ")[0])
#   df['Street'] = df['STREET'].apply(lambda x: " ".join(x.split(" ")[1:]))
#   df['Industry'] =df.apply(lambda row: "FSE-"+str(row['ESTABTYPE1'])+','+str(row['ESTABTYPE2'])+','+str(row['ESTABTYPE3'])+','+str(row['ESTABTYPE4'])+','+str(row['ESTABTYPE5'])+','+str(row['ESTABTYPE6']), axis=1)

#   df['Business Name 2'] = df['Business Name 2'].astype(str)
#   df['INSP Date'] = df['INSP Date'].astype('datetime64[D]')
#   df['LIC Exp Date'] = df['LIC Exp Date'].astype('datetime64[D]')
#   df['Last INSP Date'] = df['Last INSP Date'].astype('datetime64[D]')
#   df['# FT Employees'] = df['# FT Employees'].astype(str)
#   df['# PT Employees'] = df['# PT Employees'].astype(str)
#   df['# Sq. Ft.'] = df['# Sq. Ft.'].astype(str)
#   df['Out of Business Date'] = df['Out of Business Date'].astype('datetime64[D]')

#   del df['Address']
#   del df['Agency']
#   del df['ESTABTYPE2']
#   del df['ESTABTYPE3']
#   del df['ESTABTYPE4']
#   del df['ESTABTYPE5']
#   del df['ESTABTYPE6']

#   df = type_cast(df)
#   df = clean_zip_city(df)
 
  return df

def begin_process(segment):

  if 0 in segment:
    df = instantiate_file()
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doa/temp/df-doa.p", "wb" ))

  if 1 in segment:
    df = pickle.load( open(LOCAL_LOCUS_PATH + "data/doa/temp/df-doa.p", "rb" ))
    global_counter_init(len(df))
    df = df.apply(lambda row: add_bbl(row), axis=1)
    pickle.dump(df, open(LOCAL_LOCUS_PATH + "data/doa/temp/df-doa-1.p", "wb" ))

  cleaned_file_path = LOCAL_LOCUS_PATH + "data/dos/temp/licenses.csv"
  df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        
if __name__ == '__main__':
    begin_process([0])