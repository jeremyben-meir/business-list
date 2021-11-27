#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys
import re

#######FUNCTION DEFINITIONS#########

class DOFCertificateSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dof',['certificate'], 'certificate')
        super().__init__(self.retrieve_file(file_manager),file_manager)

    def apply_template(self, df, template):
        if template == 0:
            df = df.rename(columns={'EIN':'Record ID','Vendor Type':'Industry','BBNY Date':'LIC Issue Date','Termination Date':'LIC Exp Date', 'Legal Name':'Business Name','ST DBA Name':'Business Name 2','ST Phys or Both Line 1 Address':'Business Name 3','ST Phys or Both City':'City','ST Phys or Both State':'State','ST Phys or Both Zip':'Zip',"ST Phys or Both Line 2 Address":"Address"})
        return df

    def get_template(self,df_list):
        column_0 = ['EIN','Vendor Type','BBNY Date','Termination Date','Legal Name','ST DBA Name','ST Phys or Both Line 1 Address','ST Phys or Both Line 2 Address','ST Phys or Both City','ST Phys or Both State','ST Phys or Both Country','ST Phys or Both Zip','ST Mail Line 1 Address','ST Mail Line 2 Address','ST Mail City','ST Mail State','ST Mail Country','ST Mail Zip']
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
            parse = row["Address"].split(" ")
            ind = 0 
            while ind < len(parse) and bool(re.match('^[1234567890-]+$', parse[ind])):
                ind += 1
            if ind == len(parse):
                row["Street"] = row["Address"]
            else:
                row["Building Number"] = " ".join(parse[:ind])
                row["Street"] = " ".join(parse[ind:])
            return row
        
        self.df['Address'] = self.df['Address'].astype(str)
        self.df['Zip'] = self.df['Zip'].astype(str)
        self.df['Zip'] = self.df['Zip'].apply(lambda zipcode: zipcode[:5] if len(zipcode)>=5 else zipcode)

        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df["Contact Phone"] = ""

        self.df = self.df.apply(lambda row : clean_addr(row), axis=1)

        self.df['Business Name 3'] = self.df['Business Name 3'].astype(str)
        self.df['Industry'] = self.df['Industry'].astype(str)
        self.df['LIC Issue Date'] = self.df['LIC Issue Date'].astype('datetime64[D]')
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')

        vendor_dict = {'A':'Forced Vendor - Non-Filer','D':'Forced Vendor','E':'Entertainment Vendor','F':'Show Vendor','G':'Purged - Legacy','P':'Peddler (Sidewalk Vendor)','R':'Regular Vendor','S':'Casual Sales','T':'Temporary Vendor','U':'Use Tax'}
        self.df["Industry"] = self.df["Industry"].replace(vendor_dict.keys(),vendor_dict.values())

        for column in ['ST Mail Line 1 Address','ST Mail Line 2 Address','ST Mail City','ST Mail State','ST Mail Country','ST Mail Zip','ST Phys or Both Country']:
            del self.df[column]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOFCertificateSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()