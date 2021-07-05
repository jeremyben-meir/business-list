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
            stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
            stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]
            endwords = ["AVE","AVENUE","ST","STREET","ROAD","RD","PLACE","PL","BOULEVARD","BLVD"]
           
            if len(row['Address'])==1:
                row['Address'] = row['Address'][0]
                row['Zip'] = row['Address'].split(', NY     ')[-1][:5]
                row['City'] = row['Address'].split('     ')[-2].replace(', NY','')
                row['Street'] = row['Address'].split(', NY     ')[-2].split(' ')
                row['Street'] = (' ').join(row['Street'][:-1])
            
            else:
                row['Zip'] = row['Address'][-2].split('     ')[-1][:5]
                row['City'] = row['Address'][-2].split('     ')[0].replace(', NY','')
                
                if row['Address'][-3].split(' ')[0][:2].isdigit():
                    row['Building Number'] = row['Address'][-3].split(' ')[0].split('/')[0]
                    street = row['Address'][-3].split(' ')[1:-1]
                    street = (' ').join(street)

                    for word in stopwords1:
                        if word in street.upper():
                            street = street[:street.upper().index(word)]

                    if any(word in street.upper() for word in stopwords):
                        mylist = street.split(" ")
                        for x in range(0, len(mylist)):
                            if mylist[x].upper() in stopwords:
                                mylist = mylist[:x]
                                street = " ".join(mylist)
                                break
                        
                    mylist = street.split(" ")
                    for x in range(0, len(mylist)):
                        if mylist[x].upper() in endwords:
                            mylist = mylist[:x+1]
                            street = " ".join(mylist)
                            break

                    row['Street'] = street
                
                else:
                    row['Building Number'] = ''
                    row['Street']= row['Address'][-3]
            
            row['Building Number'] = row['Building Number'].strip(" ")
            row['Street'] = row['Street'].strip(" ")
            row['Zip'] = row['Zip'].strip(" ")

            return row
        
        self.df = self.df.rename(columns={"Registration No": "Record ID", 'Legal Name':'Business Name', 'Trade Name':'Business Name 2', 'Establishment Status':'LIC Status','Date First Registered':'LIC First Start Date', 'Registration Begins':'LIC Start Date','Registered through':'LIC Exp Date', 'Type':'Industry', 'Successor':'Record ID Successor'})


        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df["Zip"] = ""
        self.df['State'] = 'NY'
        
        self.df["Address"] = self.df["Address"].apply(lambda x : x.split("\n"))
        self.df = self.df.apply(lambda row : clean_addr(row), axis=1)
        
        self.df['Contact Phone'] = ''
        self.df['Record ID Successor'] = self.df['Record ID Successor'].astype(str)        
        self.df['Record ID Successor'] = self.df['Record ID Successor'].apply(lambda x: x.split(' ')[0])
        self.df['Record ID Successor'] = self.df['Record ID Successor'].apply(lambda x: str(int(float(x))) if x[:4].isdigit() else '' )
        self.df['Industry'] = self.df['Industry'].astype(str)
        self.df['Industry'] = self.df['Industry'].replace('PHARMACY','Pharmacy')
        self.df['Industry'] = self.df['Industry'].replace(['WHOLESALER','WHOLESALER/REPACKER'], 'Drug Wholesaler')
        self.df = self.df[~(self.df['Industry']=='MANUFACTURER') | ~(self.df['Industry']=='OUTSOURCE FACILITY')]
        self.df['LIC Start Date'] = self.df['LIC Start Date'].apply(lambda x: '' if x=='Not on file' else x)
        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        # self.df['LIC First Start Date'] = self.df['LIC First Start Date'].astype('datetime64[D]')
        self.df['LIC Start Date'] = self.df['LIC Start Date'].astype('datetime64[D]')
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')
        self.df['LIC Status'] = self.df['LIC Status'].astype(str)
        self.df['Record ID Successor'] = self.df['Record ID Successor'].astype(str)

        # self.type_cast()
        # self.clean_zip_city()
        # self.df = self.df.drop_duplicates()

        # self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOEPharmacySrcFile()
    source.instantiate_file()
    # source.add_bbl_async()
    source.save_csv()