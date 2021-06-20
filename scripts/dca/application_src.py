#######IMPORTS#######

from classes.file_manager import FileManager
from classes.source_file import SourceFile, pd

#######FUNCTION DEFINITIONS#########

class DCAApplicationSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['applications'], 'applications')
        super().__init__(self.retrieve_file(file_manager),file_manager)

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df()

        df_98_21 = df_list[1]
        df_00_12 = df_list[0]
        
        ##### DATE FORMATTING FOR 00_12 FILE AS STRING AND THEN CONVERSION INTO DATETIME BEFORE FILE MERGE

        df_00_12['End Date'] = df_00_12['End Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
        df_00_12['Start Date'] = df_00_12['Start Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
        df_00_12['Status Date'] = df_00_12['Status Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
        df_00_12['License Start'] = df_00_12['License Start'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
        df_00_12['Expiration Date'] = df_00_12['Expiration Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
        
        return pd.concat([df_98_21,df_00_12], ignore_index=True).sample(1231)

    def instantiate_file(self):

        self.df = self.df.rename(columns={"License Number": "Record ID", "Expiration Date": "License Expiration Date", "License Category": "Industry", "Start Date": "APP Start Date", "End Date": "APP End Date", "Status": "APP Status", "Status Date": "APP Status Date", "License Start": "License Start Date"})

        og = ['118','124','101','49','100','9','50','94','78','34','5','86','18','6','64','73','98','107','109','80','125','46','122','75','123','1','33','127','24','13','15','87','21','110','16','66','120','91','102','62','14']
        replace = ['Scrap Metal Processor - 118','Tow Truck Company - 124','Home Improvement Salesperson - 101','Amusement Device Temporary','Garage - 049','Home Improvement Contractor - 100','Parking Lot - 050','General Vendor - 094','Sightseeing Bus - 078','Employment Agency - 034','Secondhand Dealer Auto - 005','Horse Drawn Cab Driver - 086','Amusement Device (Portable) - 018','Secondhand Dealer [General] - 006','Laundry - 064','Cabaret - 073','Garage & Parking Lot - 098','Scale Dealer/Repairer - 107','Process Server (Organization) - 109','Pawnbroker - 080','Tow Truck Driver - 125','Pool or Billiard Room - 046','Debt Collection Agency - 122','Catering Establishment - 075','Motion Picture Projectionist - 123','Electronic Store - 001','Stoop Line Stand - 033','Cigarette Retail Dealer - 127','Newsstand - 024','Sidewalk Cafe - 013','Electronic & Home Appliance Service Dealer - 115','Horse Drawn Cab Owner - 087','Sightseeing Guide - 021','Process Server (Individual) - 110','Amusement Device (Permanent) - 016','Laundry Jobber - 066','Storage Warehouse - 120','Commercial Lessor (Bingo/Games Of Chance) - 091','Special Sale - 102','Locksmith - 062','Amusement Arcade - 014']
        self.df["Industry"] = self.df["Industry"].replace(og,replace)
        self.df['APP End Date'] = self.df['APP End Date'].astype('datetime64[D]')
        self.df['APP Start Date'] = self.df['APP Start Date'].astype('datetime64[D]')
        self.df['APP Status Date'] = self.df['APP Status Date'].astype('datetime64[D]')
        self.df['License Start Date'] = self.df['License Start Date'].astype('datetime64[D]')
        self.df['License Expiration Date'] = self.df['License Expiration Date'].astype('datetime64[D]')
        self.df['APP Status'] = self.df['APP Status'].astype(str)
        self.df['License Type'] = self.df['License Type'].astype(str)
        self.df['Application ID'] = self.df['Application ID'].astype(str)
        self.df['Application or Renewal'] = self.df['Application or Renewal'].astype(str)
        self.df['Temp Op Letter Issued'] = self.df['Temp Op Letter Issued'].astype('datetime64[D]')
        self.df['Temp Op Letter Expiration'] = self.df['Temp Op Letter Expiration'].astype('datetime64[D]')
        self.df['Unit Type'] = self.df['Unit Type'].astype(str)
        self.df['Unit'] = self.df['Unit'].astype(str)
        self.df['Description'] = self.df['Description'].astype(str)
        self.df['Street 2'] = self.df['Street 2'].astype(str)

        del self.df["Application Category"]
        del self.df["Last Update Date"]
        del self.df["Longitude"]
        del self.df["Latitude"]
        
        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCAApplicationSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()