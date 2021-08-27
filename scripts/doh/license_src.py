#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DOHLicenseSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('doh',['license'], 'license')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 0:
            pass
        return df

    def get_template(self,df_list):
        column_0 = ['entity_id','dba','address','borough','city','zipcode','BBL','seats','number_workers','chain_restaurant','CUISINECODE','CUISINE','SERVICECODE','ServiceDescription','VENUECODE','venue','Start_Date','Stop_date','currently_OOB']
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
            address = row['address'].replace('\\','').replace('_','').replace('&','').replace('  ','')
            split = address.split(' ')
            if split[0][:1].isdigit():
                if split[1] == '1/2':
                    row['Building Number'] = (' ').join(split[0:2])
                    row['Street'] = (' ').join(split[2:])
                else:
                    row['Building Number'] = split[0]
                    row['Street'] = (' ').join(split[1:])
            else:
                row['Building Number'] = ''
                row['Street'] = (' ').join(split)
            row = self.stop_end_words(row)
            return row
        
        self.df = self.df.rename(columns={'entity_id':'Record ID','dba':'Business Name','borough':'City','zipcode':'Zip','seats':'Seats','number_workers':'# FT Workers','chain_restaurant':'Chain Flag','Start_Date':'LIC Start Date','Stop_date':'LIC Exp Date'})
        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df['State'] = 'NY'
        
        self.df = self.df.apply(lambda x: clean_addr(x), axis=1)

        self.df['Seats'] = self.df['Seats'].astype(str)
        self.df['# FT Workers'] = self.df['# FT Workers'].astype(str)
        self.df['Chain Flag'] = self.df['Chain Flag'].astype(str)
        self.df['Industry'] = self.df.apply(lambda x: 'Restaurant - {}, {}, {}'.format(x['CUISINE'],x['ServiceDescription'],x['venue']), axis =1)
        self.df['LIC Start Date'] = self.df['LIC Start Date'].astype('datetime64[D]')
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')
        self.df['Contact Phone'] = ''
        self.df = self.df.replace('NULL','')
        
        del self.df['address']
        del self.df['city']
        del self.df['CUISINE']
        del self.df['ServiceDescription']
        del self.df['venue']
        del self.df['CUISINECODE']
        del self.df['SERVICECODE']
        del self.df['VENUECODE']
        del self.df['currently_OOB']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOHLicenseSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()