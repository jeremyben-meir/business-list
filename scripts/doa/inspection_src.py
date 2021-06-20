#######IMPORTS#######

from classes.file_manager import FileManager
from classes.source_file import SourceFile, pd

#######FUNCTION DEFINITIONS#########

class DOAInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('doa',['inspections','main','inspections-onsite','deficiencies'],'inspections')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df()

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

        return pd.concat([df_1,df_2,df_3,df_master[master_columns]], axis=0, join='outer', ignore_index=False)

    def instantiate_file(self):

        self.df = self.df.rename(columns={"ESTABNO": "Record ID", 'TRADENAME':'Business Name','OWNNAME':'Business Name 2', 'CITY':'City', 'ZIP':'Zip','PHONE':'Contact Phone', 'INSPDATE':'INSP Date',	'LICDATE':'LIC Exp Date',	'SANDATE01':'Last INSP Date',	'NOFTEMP':'# FT Employees',	'NOPTEMP':'# PT Employees',	'NOSQFT':'# Sq. Ft.',	'OOBDATE':'Out of Business Date'})
    
        self.df['State'] ='NY'
        self.df['STREET'] = self.df['STREET'].apply(lambda x: str(x).replace('#',''))
        self.df['Building Number'] = self.df['STREET'].apply(lambda x: x.split(" ")[0])
        self.df['Street'] = self.df['STREET'].apply(lambda x: " ".join(x.split(" ")[1:]))
        self.df['Industry'] = self.df.apply(lambda row: f"FSE-{row['ESTABTYPE1']},{row['ESTABTYPE2']},{row['ESTABTYPE3']},{row['ESTABTYPE4']},{row['ESTABTYPE5']},{row['ESTABTYPE6']}", axis=1)

        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        self.df['INSP Date'] = self.df['INSP Date'].astype('datetime64[D]')
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')
        self.df['Last INSP Date'] = self.df['Last INSP Date'].astype('datetime64[D]')
        self.df['# FT Employees'] = self.df['# FT Employees'].astype(str)
        self.df['# PT Employees'] = self.df['# PT Employees'].astype(str)
        self.df['# Sq. Ft.'] = self.df['# Sq. Ft.'].astype(str)
        # df['Out of Business Date'] = df['Out of Business Date'].astype('datetime64[D]')

        del self.df['STREET']
        del self.df['ESTABTYPE1']
        del self.df['ESTABTYPE2']
        del self.df['ESTABTYPE3']
        del self.df['ESTABTYPE4']
        del self.df['ESTABTYPE5']
        del self.df['ESTABTYPE6']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOAInspectionSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()