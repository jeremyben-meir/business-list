#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd

#######FUNCTION DEFINITIONS#########

class DOHInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('doh',['inspections'], 'inspections')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        return df

    def get_template(self,df_list):
        for df_val in range(len(df_list)):
            df = df_list[df_val]
            if df.columns == ["a","b","c","d","e","f","g"]:
                df_list[df_val] = self.apply_template(df,0)
            elif df.columns == ["a","z","c","d","e","f","g"]:
                df_list[df_val] = self.apply_template(df,1)
        return df_list

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df() #[df0,df1,df2,df3,df4]
        df_list = self.get_template(df_list) ## [1,1,2,0,1]
        return pd.concat(df_list, ignore_index=True)

    def instantiate_file(self):
        self.df = self.df.rename(columns={"CAMIS": "Record ID", 'DBA':'Business Name', 'BORO':'City', 'ZIPCODE':'Zip','PHONE':'Contact Phone', 'CUISINECODE':'Industry','INSPDATE':'INSP Date','CASE_DECISION_DATE':'Case Dec. Date','CURRENTGRADE':'Current Grade','GRADEDATE':'Current Grade Date'})
        
        self.df['City'] = self.df['City'].replace(['0','1','2','3',"4","5"],['','NEW YORK', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND'])
        self.df['Building Number'] = self.df['ADDRESS'].apply(lambda x: x.split(" ")[0])
        self.df['Street'] = self.df['ADDRESS'].apply(lambda x: " ".join(x.split(" ")[1:]))
        self.df['Industry'] = self.df['Industry'].apply(lambda x: "Restaurant-"+x)
        self.df['Industry'] = self.df['Industry'].apply(lambda x: x.strip('        '))
        self.df['State'] = 'NY'
        self.df['INSP Date'] = self.df['INSP Date'].astype('datetime64[D]')
        self.df['Case Dec. Date'] = self.df['Case Dec. Date'].replace(['N/A','NULL','nan'],'')
        self.df['Case Dec. Date'] = self.df['Case Dec. Date'].astype('datetime64[D]')
        self.df['Current Grade'] = self.df['Current Grade'].astype(str)
        self.df['Current Grade Date'] = self.df['Current Grade Date'].replace(['N/A','NULL','nan'],'')
        self.df['Current Grade Date'] = self.df['Current Grade Date'].astype('datetime64[D]')

        del self.df['ADDRESS']
        del self.df['ACTION']
        del self.df['PROGRAM']
        del self.df['INSPTYPE']
        del self.df['VIOLCODE']
        del self.df['DISMISSED']
        del self.df['SCORE']
        del self.df['VIOLSCORE']
        del self.df['MOD_TOTALSCORE']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOHInspectionSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()