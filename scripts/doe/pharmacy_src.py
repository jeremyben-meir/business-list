#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DOEPharmacySrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('doe',['pharmacy'],'pharmacy')
        super().__init__(self.retrieve_file(file_manager), file_manager)
   
    def apply_template(self, df, template):
        if template == 0:
            pass
        return df

    def get_template(self,df_list):
        column_0 = ['Type', 'Legal Name', 'Trade Name', 'Registration No', 'Date First Registered', 'Registration Begins', 'Registered through', 'Establishment Status', 'Successor', 'Address']
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
            row['Zip'] = row['Address'][-1].split('     ')[-1][:5]
            row['City'] = row['Address'][-1].split('     ')[0].replace(', NY','')
            
            if row['Address'][0].split(' ')[0][:2].isdigit():
                row['Building Number'] = row['Address'][0].split(' ')[0].split('/')[0]
                row['Street'] = " ".join(row['Address'][0].split(' ')[1:])
                row = self.stop_end_words(row)
            else:
                row['Building Number'] = ''
                row['Street']= row['Address'][0]
            
            row['Building Number'] = row['Building Number'].strip(" ")
            row['Street'] = row['Street'].strip(" ")
            row['Zip'] = row['Zip'].strip(" ")

            return row
        
        self.df = self.df.rename(columns={"Registration No": "Record ID", 'Legal Name':'Business Name', 'Trade Name':'Business Name 2', 'Establishment Status':'LIC Status','Date First Registered':'LIC Issue Date', 'Registration Begins':'LIC Start Date','Registered through':'LIC Exp Date', 'Type':'Industry', 'Successor':'Record ID 2'})


        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df["Zip"] = ""
        self.df['State'] = 'NY'

        def addr_list(address_list):
            address_list = address_list.strip().strip("\n").split("\n")
            address_list = [address.strip() for address in address_list]
            delelt = list()
            for elt in range(len(address_list)):
                if "AIRPORT" in address_list[elt] or "AIRLINE" in address_list[elt]or "JFK" in address_list[elt] or "LAGUARDIA" in address_list[elt] or "LA GUARDIA" in address_list[elt]:
                    pass
                elif len(address_list[elt]) < 5 or not any (char.isdigit() for char in address_list[elt]):
                    delelt.append(elt)
            address_list = [i for j, i in enumerate(address_list) if j not in delelt]
            return address_list
        
        self.df["Address"] = self.df["Address"].apply(lambda x : addr_list(x))
        self.df = self.df.apply(lambda row : clean_addr(row), axis=1)
        
        self.df['Contact Phone'] = ''
        self.df['Record ID 2'] = self.df['Record ID 2'].astype(str)        
        self.df['Record ID 2'] = self.df['Record ID 2'].apply(lambda x: x.split(' ')[0])
        self.df['Record ID 2'] = self.df['Record ID 2'].apply(lambda x: str(int(float(x))) if x[:4].isdigit() else '' )
        self.df['Industry'] = self.df['Industry'].astype(str)
        self.df['Industry'] = self.df['Industry'].replace('PHARMACY','Pharmacy')
        self.df['Industry'] = self.df['Industry'].replace(['WHOLESALER','WHOLESALER/REPACKER'], 'Drug Wholesaler')
        self.df = self.df[~(self.df['Industry']=='MANUFACTURER') | ~(self.df['Industry']=='OUTSOURCE FACILITY')]
        self.df['LIC Start Date'] = self.df['LIC Start Date'].apply(lambda x: '' if x=='Not on file' else x)
        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        self.df['LIC Issue Date'] = pd.to_datetime(self.df['LIC Issue Date'], errors='coerce')
        self.df['LIC Start Date'] = pd.to_datetime(self.df['LIC Start Date'], errors='coerce')
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')
        self.df['LIC Status'] = self.df['LIC Status'].astype(str)
        self.df['Record ID 2'] = self.df['Record ID 2'].astype(str)

        del self.df["Address"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOEPharmacySrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()