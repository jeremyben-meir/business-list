#######IMPORTS#######

from classes.file_manager import FileManager
from classes.source_file import SourceFile, pd

#######FUNCTION DEFINITIONS#########

class DCAInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['inspections'], 'inspections')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df()

        # Get df from file paths
        df_10_17 = df_list[1]
        df_14_21 = df_list[0]

        # Adjust column headers to match
        # col_10_17 = ['REC_ID','CERT_NBR','BIZ_NAME','INSP_DT','INSP_RSLT','INDUSTRY','BORO','BLDG_NBR','STREET','STREET2','UNIT_TYP','UNIT','DESCR','CITY','STATE','ZIP','X_COORD','Y_COORD']
        col_14_21 = ['Record ID','Certificate Number','Business Name','Inspection Date','Inspection Result','Industry','Borough','Building Number','Street','Street 2','Unit Type','Unit','Description','City','State','Zip','Longitude','Latitude']
        df_10_17.columns = col_14_21

        # Concatenate the two files
        return pd.concat([df_10_17,df_14_21], ignore_index=True)

    def instantiate_file(self):
        # Rename appropriate rows
        self.df = self.df.rename(columns={"Inspection Date": "INSP Date", 'Inspection Result':"INSP Result"})

        business_indicators_noninformative = ["Business Padlocked", "Unable to Locate", "Unable to Complete Inspection", "Unable to Seize Vehicle"]

        self.df['Contact Phone'] = ""
        self.df["INSP Result"] = self.df["INSP Result"].astype(str)
        self.df = self.df[~self.df["INSP Result"].isin(business_indicators_noninformative)]
        self.df["INSP Date"] = self.df["INSP Date"].astype('datetime64[D]')
        self.df['Street 2'] = self.df['Street 2'].astype(str)
        self.df['Unit Type'] = self.df['Unit Type'].astype(str)
        self.df['Unit'] = self.df['Unit'].astype(str)
        self.df['Description'] = self.df['Description'].astype(str)

        del self.df["Certificate Number"]
        del self.df["Borough"]
        del self.df["Longitude"]
        del self.df["Latitude"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCAInspectionSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()