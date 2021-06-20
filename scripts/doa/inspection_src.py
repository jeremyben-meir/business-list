#######IMPORTS#######

from classes.common import DirectoryFields
from classes.file_retriever import FileRetriever
from classes.source_file import SourceFile, pd, pickle, csv

#######FUNCTION DEFINITIONS#########

def instantiate_file(source):
    df_list = FileRetriever('doa','inspections').retrieve_df()
    df_list.append(FileRetriever('doa','main').retrieve_df())
    df_list.append(FileRetriever('doa','inspection-onsite').retrieve_df())
    df_list.append(FileRetriever('doa','deficiencies').retrieve_df())
    df = pd.concat(df_list, ignore_index=True)

    # Get dfs from file paths
    df_insp = df_list[0]
    df_master = df_list[1]
    df_insp_os = df_list[2]
    df_def = df_list[3]
    
    # Concatenate the dfs
    master_columns = ['ESTABNO','SANDATE01','NOFTEMP','NOPTEMP','NOSQFT','OOBDATE','OWNNAME','TRADENAME','STREET','CITY','ZIP','PHONE','ESTABTYPE1','ESTABTYPE2','ESTABTYPE3','ESTABTYPE4','ESTABTYPE5','ESTABTYPE6']
    
    df_1 = pd.merge(df_insp[['ESTABNO','ZIP','INSPDATE','LICDATE']], df_master[master_columns], how='left', on = ['ESTABNO','ZIP'])
    df_2 = pd.merge(df_insp_os[['ESTABNO','INSPDATE']], df_master[master_columns], how='left', on = ['ESTABNO'])
    df_3 = pd.merge(df_insp_os[['ESTABNO','INSPDATE']], df_master[master_columns], how='left', on = ['ESTABNO'])

    df = pd.concat([df_1,df_2,df_3,df_master[master_columns]], axis=0, join='outer', ignore_index=False)

    df = df.rename(columns={"ESTABNO": "Record ID", 'TRADENAME':'Business Name','OWNNAME':'Business Name 2', 'CITY':'City', 'ZIP':'Zip','PHONE':'Contact Phone', 'INSPDATE':'INSP Date',	'LICDATE':'LIC Exp Date',	'SANDATE01':'Last INSP Date',	'NOFTEMP':'# FT Employees',	'NOPTEMP':'# PT Employees',	'NOSQFT':'# Sq. Ft.',	'OOBDATE':'Out of Business Date'})
  
    df['State'] ='NY'
    df['STREET'] = df['STREET'].apply(lambda x: str(x).replace('#',''))
    df['Building Number'] = df['STREET'].apply(lambda x: x.split(" ")[0])
    df['Street'] = df['STREET'].apply(lambda x: " ".join(x.split(" ")[1:]))
    df['Industry'] =df.apply(lambda row: f"FSE-{row['ESTABTYPE1']},{row['ESTABTYPE2']},{row['ESTABTYPE3']},{row['ESTABTYPE4']},{row['ESTABTYPE5']},{row['ESTABTYPE6']}", axis=1)

    df['Business Name 2'] = df['Business Name 2'].astype(str)
    df['INSP Date'] = df['INSP Date'].astype('datetime64[D]')
    df['LIC Exp Date'] = df['LIC Exp Date'].astype('datetime64[D]')
    df['Last INSP Date'] = df['Last INSP Date'].astype('datetime64[D]')
    df['# FT Employees'] = df['# FT Employees'].astype(str)
    df['# PT Employees'] = df['# PT Employees'].astype(str)
    df['# Sq. Ft.'] = df['# Sq. Ft.'].astype(str)
    # df['Out of Business Date'] = df['Out of Business Date'].astype('datetime64[D]')

    del df['STREET']
    del df['ESTABTYPE1']
    del df['ESTABTYPE2']
    del df['ESTABTYPE3']
    del df['ESTABTYPE4']
    del df['ESTABTYPE5']
    del df['ESTABTYPE6']

    df = source.type_cast(df)
    df = source.clean_zip_city(df)
    df = df.drop_duplicates()
  
    return df

def begin_process(segment):
    source = SourceFile()

    if 0 in segment:
        df = instantiate_file(source)
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/df-doa.p", "wb" ))

    if 1 in segment:
        df = pickle.load( open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/df-doa.p", "rb" ))
        df = source.add_bbl_async(df)
        pickle.dump(df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/df-doa-1.p", "wb" ))

    cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/doa/temp/inspections.csv"
    df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)
        
if __name__ == '__main__':
    begin_process([0])