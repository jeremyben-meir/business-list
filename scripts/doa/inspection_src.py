#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DOAInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('doa',['inspection','main','inspection-onsite','deficiency'],'inspection')
        super().__init__(self.retrieve_file(file_manager), file_manager)
   
    def apply_template(self, df, template):
        if template == 0:
            df = df[['ESTABNO','ZIP','INSPDATE','LICDATE']]
        if template == 1:
            df = df[['ESTABNO','SANDATE01','NOFTEMP','NOPTEMP','NOSQFT','OOBDATE','OWNNAME','TRADENAME','STREET','CITY','ZIP','PHONE','ESTABTYPE1','ESTABTYPE2','ESTABTYPE3','ESTABTYPE4','ESTABTYPE5','ESTABTYPE6']]
        if template == 2:
            df = df[['ESTABNO','INSPDATE']]
        if template == 3:
            df = df[['ESTABNO','INSPDATE']]
        return df

    def get_template(self,df_list):
        column_0 = ['ESTABNO', 'INSPDATE', 'BEGTIME', 'INSPTYPE', 'SCORE', 'FDACONTR', 'PHOTOS', 'COMPLAINT', 'RECNAME', 'RECTITLE', 'OPWITHOUT', 'MEMO', 'LICTYPE', 'LICDATE', 'OWNNAME', 'TRADENAME', 'STREET', 'CITY', 'ZIP', 'ESTABTYPE1', 'ESTABTYPE2', 'ESTABTYPE3', 'ESTABTYPE4', 'ESTABTYPE5', 'ESTABTYPE6', 'IPHCODE', 'PRINTED', 'BACKSYNC', 'LIMITEDIND', 'CRITDEFFIX']
        column_1 = ['ESTABNO', 'OWNNAME', 'TRADENAME', 'STREET', 'CITY', 'ZIP', 'PHONE', 'ESTABTYPE1', 'ESTABTYPE2', 'ESTABTYPE3', 'ESTABTYPE4', 'ESTABTYPE5','ESTABTYPE6', 'PROCCODE1', 'COMMCODE1', 'PROCCODE2', 'COMMCODE2','PROCCODE3', 'COMMCODE3', 'PROCCODE4', 'COMMCODE4', 'PROCCODE5','COMMCODE5', 'PROCCODE6', 'COMMCODE6', 'SANDATE01', 'SANSCORE01','SANTYPE01', 'IPHCODE', 'VAR1CODE', 'VAR2CODE', 'SCANIND', 'PRODIND','EGGSIND', 'WATERSOURC', 'HOLDIND', 'UNITPRICE', 'CHAINNO', 'OPERSU','OPERMO', 'OPERTU', 'OPERWE', 'OPERTH', 'OPERFR', 'OPERSA', 'OPEROPEN','OPERAPOPN', 'OPERCLOSE', 'OPERAPCLS', 'OPERBEGMM', 'OPERENDMM', 'NOFTEMP', 'NOPTEMP', 'NOSQFT', 'NOCHKOUT', 'CERTDATE', 'OOBDATE','REGION', 'ZONE', 'TRANSTIME', 'LIMITEDIND']
        column_2 = ['ESTABNO', 'INSPDATE', 'BEGTIME', 'INSPNO', 'TIMEIN', 'TIMEOUT','TOTALTIME']
        column_3 = ['ESTABNO', 'INSPDATE', 'BEGTIME', 'DEFNO', 'DEFTEXT']

        df_insp = pd.DataFrame()
        df_main = pd.DataFrame()
        df_insp_os = pd.DataFrame()
        df_def = pd.DataFrame()

        for df_val in range(len(df_list)):
            df = df_list[df_val]
            columns = df.columns.tolist()
            if columns == column_0:
                df = self.apply_template(df,0)
                df_insp = pd.concat([df_insp,df], ignore_index=True)
                df_insp = df_insp.drop_duplicates()
            elif columns == column_1:
                df = self.apply_template(df,1)
                df_main = pd.concat([df_main,df], ignore_index=True)
                df_main = df_main.drop_duplicates()
            elif columns == column_2: 
                df = self.apply_template(df,2)
                df_insp_os= pd.concat([df_insp_os,df], ignore_index=True)
                df_insp_os = df_insp_os.drop_duplicates()
            elif columns == column_3:
                df = self.apply_template(df,3)
                df_def = pd.concat([df_def,df], ignore_index=True)
                df_def = df_def.drop_duplicates()
            else:
                sys.exit('Columns do not match any templates.')
        
        df_insp = pd.merge(df_insp, df_main, how='left', on = ['ESTABNO','ZIP'])
        df_insp_os = pd.merge(df_insp_os, df_main, how='left', on = ['ESTABNO'])
        df_def = pd.merge(df_def, df_main, how='left', on = ['ESTABNO'])
        df = pd.concat([df_insp,df_insp_os,df_def,df_main], axis=0, join='outer', ignore_index=False)

        df_list = [df]

        return df_list

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df() 
        df_list = self.get_template(df_list) 
        return pd.concat(df_list, ignore_index=True)

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
        self.df['Out of Business Date'] = self.df['Out of Business Date'].astype('datetime64[D]')

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
    # source.add_bbl_async()
    source.save_csv()