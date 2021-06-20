#######IMPORTS#######

from classes.file_manager import FileManager
from classes.source_file import SourceFile, pd

#######FUNCTION DEFINITIONS#########

class DOHInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dos',['aes','barber'], 'licenses')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def retrieve_file(self,file_manager):
        # Get file paths
        df_list = file_manager.retrieve_df()

        # Get dfs from file paths
        df_aes = df_list[0]
        df_barber = df_list[1]

        # Concatenate the dfs  
        df_aes['Industry'] = 'Appearance Enhancement Service'
        df_barber['Industry'] = 'Barber'
        return pd.concat([df_aes,df_barber], axis=0, join='outer', ignore_index=False)

    def instantiate_file(self):

        def clean_addr(row):
            stopwords = ['FL', 'STORE', 'BSMT','RM','UNIT','STE','APT','APT#','FRNT','#','MEZZANINE','LOBBY','GROUND','FLOOR','SUITE','LEVEL']
            stopwords1 = ["1ST FLOOR", "1ST FL","2ND FLOOR", "2ND FL","3RD FLOOR", "3RD FL","4TH FLOOR", "4TH FL","5TH FLOOR", "5TH FL","6TH FLOOR", "6TH FL","7TH FLOOR", "7TH FL","8TH FLOOR", "8TH FL","9TH FLOOR", "9TH FL","10TH FLOOR", "10TH FL"]
            endwords = ["AVE","AVENUE","ST","STREET","ROAD","RD","PLACE","PL","BOULEVARD","BLVD"]
            
            # address = row['Address'][0]
            # zipcode = row['Address'][1]

            if not any(x.isdigit() for x in row['Address'][0]):
                del row['Address'][0]
            if "United States" in row['Address'][-1]:
                del row['Address'][-1]

            if len(row['Address']) > 1:
                # print(row['Address'][0])
                row['Address'][0] = row['Address'][0].replace(",","")
                row['Address'][0] = row['Address'][0].replace(".","")
                row['Address'][0] = row['Address'][0].replace("\"","")
                row['Address'][0] = row['Address'][0].replace("-","")
                row['Address'][0] = row['Address'][0].strip(" ")
                
            
                for word in stopwords1:
                    if word in row['Address'][0].upper():
                        row['Address'][0] = row['Address'][0][:row['Address'][0].upper().index(word)]

                if any(word in row['Address'][0].upper() for word in stopwords):
                    mylist = row['Address'][0].split(" ")
                    for x in range(0, len(mylist)):
                        if mylist[x].upper() in stopwords:
                            mylist = mylist[:x]
                            row['Address'][0] = " ".join(mylist)
                            break
                    
                mylist = row['Address'][0].split(" ")
                for x in range(0, len(mylist)):
                    if mylist[x].upper() in endwords:
                        mylist = mylist[:x+1]
                        row['Address'][0] = " ".join(mylist)
                        break

                try:
                    if row['Address'][0].split(" ")[1][:-1].isnumeric() or row['Address'][0].split(" ")[1].isnumeric():
                        mylist = row['Address'][0].split(" ")
                        mylist[1] = mylist[0] + "-" + mylist[1]
                        del mylist[0]
                        row['Address'][0] = " ".join(mylist)
                except:
                    pass   

                if not " " in row['Address'][0]:
                    row['Building Number']=""
                    row['Street']=""
                else:
                    row['Building Number'] = row['Address'][0].split(" ")[0]
                    row['Street'] = " ".join(row['Address'][0].split(" ")[1:])

                if not (row['Building Number'].replace("-","").isnumeric()):
                    row['Building Number']=""
                    row['Street']=""

                row['Zip'] = row['Address'][1].split(" ")[-1]
                
                if "1/2" in row['Street']:
                    row['Street'] = row['Street'][row['Street'].index("1/2")+3:]
            
                if not row['Zip'].isnumeric():
                    if "-" in row["Zip"]:
                        row["Zip"] = row["Zip"].split("-")[0]
                    if row['Address'][-1][-1] == " ":
                        row['Address'][-1] = row['Address'][-1][:-1]
                    row['Zip'] = row['Address'][-1].split(" ")[-1][:5]
            
            row['Building Number'] = row['Building Number'].strip(" ")
            row['Street'] = row['Street'].strip(" ")
            row['Zip'] = row['Zip'].strip(" ")

            return row

        self.df = self.df.rename(columns={"License Number": "Record ID", 'Name':'Business Name 2', 'County':'City', 'License State':'State' ,'Phone':'Contact Phone', 'License Issue Date':'LIC Issue Date', 'Current Term Effective Date':'LIC Current Issue Date', 'Expiration Date':'LIC Exp Date', 'License Status':'LIC Status'})
        
        self.df["Record ID"] = self.df["Record ID"].apply(lambda x : x.replace(" ",""))
        self.df["Record ID"] = self.df["Record ID"].astype(str)
        self.df["Business Name 2"] = self.df["Business Name 2"].astype(str)

        self.df["Business Name 2"]=self.df["Business Name 2"].apply(lambda x:x.replace(u'\xa0', u' '))
        self.df["Address"]=self.df["Address"].apply(lambda x:x.replace(u'\xa0', u' '))

        self.df["Address"] = self.df["Address"].apply(lambda x : x.split("                "))

        self.df["Building Number"] = ""
        self.df["Street"] = ""
        self.df["Zip"] = ""

        self.df = self.df.apply(lambda row : clean_addr(row), axis=1)

        del self.df["Address"]
        del self.df["Agency"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOHInspectionSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()