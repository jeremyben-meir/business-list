#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd

#######FUNCTION DEFINITIONS#########

class DCAChargeSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['charges'],'charges')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df()
        return pd.concat(df_list, ignore_index=True)

    def instantiate_file(self):

        self.df = self.df.rename(columns={"Violation Date": "CHRG Date"})

        self.df['Contact Phone'] = ""
        self.df['CHRG Date'] = self.df['CHRG Date'].astype('datetime64[D]')
        self.df['Street 2'] = self.df['Street 2'].astype(str)
        self.df['Unit Type'] = self.df['Unit Type'].astype(str)
        self.df['Unit'] = self.df['Unit'].astype(str)
        self.df['Description'] = self.df['Description'].astype(str)

        del self.df["Certificate Number"]
        del self.df["Borough"]
        del self.df["Charge"]
        del self.df["Charge Count"]
        del self.df["Outcome"]
        del self.df["Counts Settled"]
        del self.df["Counts Guilty"]
        del self.df["Counts Not Guilty"]
        del self.df["Longitude"]
        del self.df["Latitude"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCAChargeSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()