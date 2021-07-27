#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DOSLicensesSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dos',['aes','barber'], 'license')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 0:
            pass
        
        return df

    def get_template(self,df_list):
        column_0 = ['License Number', 'Name', 'Business Name', 'Address','Zip', 'Phone', 'County', 'License State', 'License Issue Date', 'Current Term Effective Date', 'Expiration Date', 'Agency', 'License Status','Industry','URL']
        
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
            
            try:
                if row["Street"].split(" ")[1][:-1].isnumeric() or row["Street"].split(" ")[1].isnumeric():
                    mylist = row["Street"].split(" ")
                    mylist[1] = mylist[0] + "-" + mylist[1]
                    del mylist[0]
                    row["Street"] = " ".join(mylist)
            except:
                pass   


            if " " in row["Street"]:
                row['Building Number'] = row["Street"].split(" ")[0]
                row['Street'] = " ".join(row["Street"].split(" ")[1:]) 

            row = self.stop_end_words(row)
            
            if "1/2" in row['Street']:
                row['Street'] = row['Street'][row['Street'].index("1/2")+3:]
            
            row['Building Number'] = row['Building Number'].strip(" ")
            row['Street'] = row['Street'].strip(" ")

            return row

        self.df = self.df.rename(columns={"License Number": "Record ID", 'Name':'Business Name 2', 'County':'City','Address':'Street', 'License State':'State' ,'Phone':'Contact Phone', 'License Issue Date':'LIC Issue Date', 'Current Term Effective Date':'LIC Start Date', 'Expiration Date':'LIC Exp Date', 'License Status':'LIC Status'})
        
        self.df["Record ID"] = self.df["Record ID"].apply(lambda x : x.replace(" ",""))
        self.df["Record ID"] = self.df["Record ID"].astype(str)
        self.df["Business Name 2"] = self.df["Business Name 2"].astype(str)

        self.df["Business Name 2"]=self.df["Business Name 2"].apply(lambda x:x.replace(u'\xa0', u' '))

        self.df["Building Number"] = ""

        self.df = self.df.apply(lambda row : clean_addr(row), axis=1)

        del self.df["Agency"]
        del self.df["URL"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOSLicensesSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()