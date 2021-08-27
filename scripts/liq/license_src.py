#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class LIQLicenseSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('liq',['liquor'], 'license')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 0:
            pass
        
        return df

    def get_template(self,df_list):
        column_0 = ['Premises Name', 'Trade Name', 'Zone', 'Address', 'Zip', 'County', 'Serial Number', 'License Type', 'License Status', 'Credit Group', 'Filing Date', 'Effective Date', 'Expiration Date', "Principal's Name", 'URL']
        
        for df_val in range(len(df_list)):
            df = df_list[df_val]
            columns = df.columns.tolist()
            if columns == column_0:
                df_list[df_val] = self.apply_template(df,0)
            else:
                sys.exit('Columns do not match any templates.')
        return df_list

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df() 
        df_list = self.get_template(df_list) 
        return pd.concat(df_list, ignore_index=True)

    def instantiate_file(self):
        def clean_addr(row):
            row['Address'] = row['Address'].strip(' ')
            row['Address'] = row['Address'].split(' ')
            if len(row['Address'])>1:    
                if row['Address'][0][0].isdigit():
                    if row['Address'][1][:-1].isdigit():
                        row['Building Number'] = ('-').join(row['Address'][:2])
                        row['Street'] = (' ').join(row['Address'][2:])
                    else:
                        row['Building Number'] = row['Address'][0]
                        row['Street'] = (' ').join(row['Address'][1:])
                else:
                    row['Building Number'] = ''
                    row['Street'] = (' ').join(row['Address'])
                
                row = self.stop_end_words(row)
            return row
        
        self.df = self.df.rename(columns={'Premises Name':'Business Name', 'Trade Name':'Business Name 2', 'County':'City', 'Serial Number':'Record ID', 'License Type':'Industry', 'License Status':'LIC Status', 'Filing Date':'LIC Filing Date', 'Effective Date':'LIC Start Date', 'Expiration Date':'LIC Exp Date'})

        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df["Contact Phone"] = ""
        self.df['State'] = 'NY'

        self.df['Address'] = self.df['Address'].astype(str)
        self.df = self.df.apply(lambda row : clean_addr(row), axis=1)

        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        self.df['LIC Status'] = self.df['LIC Status'].astype(str)
        self.df['LIC Start Date'] = pd.to_datetime(self.df['LIC Start Date'], errors = 'coerce')
        self.df['LIC Exp Date'] = pd.to_datetime(self.df['LIC Exp Date'], errors = 'coerce')
        self.df['LIC Filing Date'] = pd.to_datetime(self.df['LIC Filing Date'], errors = 'coerce')

        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].apply(lambda date: date if date.year <= 2024 else pd.NaT)

        del self.df['Address']
        del self.df['Zone']
        del self.df['Credit Group']
        del self.df['Principal\'s Name']
        del self.df['URL']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = LIQLicenseSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()