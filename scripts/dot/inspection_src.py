#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DOTInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dot',['inspection'], 'inspection')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 0:
            pass
        return df

    def get_template(self,df_list):
        column_0 = ['borough', 'restaurantname', 'seatingchoice', 'legalbusinessname', 'businessaddress', 'restaurantinspectionid', 'issidewaycompliant', 'isroadwaycompliant', 'skippedreason', 'inspectedon', 'agencycode', 'postcode', 'latitude', 'longitude', 'communityboard', 'councildistrict', 'censustract', 'bin', 'bbl', 'nta']

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
            address = row['businessaddress'].replace('\\','').replace('_','').replace('&','').replace('  ','')
            split = address.split(' ')
            if len(split) > 1:
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
            else:
                row['Building Number'] = ''
                row['Street'] = (' ').join(split)
            row = self.stop_end_words(row)
            return row

        self.df = self.df.rename(columns={'borough':'City', 'restaurantname':'Business Name', 'legalbusinessname':'Business Name 2', 'restaurantinspectionid':'Record ID', 'inspectedon':'INSP Date', 'postcode':'Zip', 'bbl':'BBL'})

        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df['State'] = 'NY'
        
        self.df = self.df.apply(lambda x: clean_addr(x), axis=1)

        self.df['Contact Phone'] = ''
        self.df['Industry'] = 'Restaurant'
        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        self.df['INSP Date'] = self.df['INSP Date'].astype('datetime64[D]')

        del self.df['businessaddress']
        del self.df['seatingchoice']
        del self.df['issidewaycompliant']
        del self.df['isroadwaycompliant']
        del self.df['skippedreason']
        del self.df['agencycode']
        del self.df['latitude']
        del self.df['longitude']
        del self.df['communityboard']
        del self.df['councildistrict']
        del self.df['censustract']
        del self.df['bin']
        del self.df['nta']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOTInspectionSrcFile()
    source.instantiate_file()
    # source.add_bbl_async(overwrite=False)
    source.save_csv()