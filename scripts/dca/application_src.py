#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DCAApplicationSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['application'], 'application')
        super().__init__(self.retrieve_file(file_manager),file_manager)

    def apply_template(self, df, template):
        if template == 0:
            df['End Date'] = df['End Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
            df['Start Date'] = df['Start Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
            df['Status Date'] = df['Status Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
            df['License Start'] = df['License Start'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
            df['Expiration Date'] = df['Expiration Date'].astype(str).apply(lambda x: x[4:6]+"/"+x[6:8]+"/"+x[:4] if len(x)==10 else x)
            df.columns = ['Application ID', 'License Number', 'License Type', 'Application or Renewal', 'Business Name', 'Status', 'Status Date', 'Start Date', 'End Date', 'License Start', 'Expiration Date', 'Temp Op Letter Issued', 'Temp Op Letter Expiration', 'License Category', 'Application Category', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Contact Phone', 'Last Update Date']
        elif template == 1:
            del df['Longitude']
            del df['Latitude']
            df.columns = ['Application ID', 'License Number', 'License Type', 'Application or Renewal', 'Business Name', 'Status', 'Status Date', 'Start Date', 'End Date', 'License Start', 'Expiration Date', 'Temp Op Letter Issued', 'Temp Op Letter Expiration', 'License Category', 'Application Category', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Contact Phone', 'Last Update Date']
        elif template == 2:
            del df['longitude']
            del df['latitude']
            del df['active_vehicles']
            df['Status Date'] = ''
            df['License Start'] = ''
            df['Expiration Date'] = ''
            df['Last Update Date'] = ''
            df = df[['application_id', 'license_number', 'license_type', 'application_or_renewal', 'business_name', 'status','Status Date', 'start_date', 'end_date', 'License Start', 'Expiration Date', 'temp_op_letter_issued', 'temp_op_letter_expiration', 'license_category', 'application_category', 'building_number', 'street', 'street_2', 'unit_type', 'unit', 'description', 'city', 'state', 'zip', 'contact_phone', 'Last Update Date']]
            df.columns = ['Application ID', 'License Number', 'License Type', 'Application or Renewal', 'Business Name', 'Status', 'Status Date', 'Start Date', 'End Date', 'License Start', 'Expiration Date', 'Temp Op Letter Issued', 'Temp Op Letter Expiration', 'License Category', 'Application Category', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Contact Phone', 'Last Update Date']
        return df

    def get_template(self,df_list):
        column_0 = ['Application ID', 'License Number', 'License Type', 'Application or Renewal', 'Business Name', 'Status', 'Status Date', 'Start Date', 'End Date', 'License Start', 'Expiration Date', 'Temp Op Letter Issued', 'Temp Op Letter Expiration', 'License Category', 'Application Category', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Contact Phone', 'Last Update Date'] 
        column_1 = ['Application ID', 'License Number', 'License Type', 'Application or Renewal', 'Business Name', 'Status', 'Status Date', 'Start Date', 'End Date', 'License Start', 'Expiration Date', 'Temp Op Letter Issued', 'Temp Op Letter Expiration', 'License Category', 'Application Category', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Contact Phone', 'Last Update Date', 'Longitude', 'Latitude']
        column_2 = ['application_id', 'license_number', 'license_type', 'application_or_renewal', 'business_name', 'status', 'start_date', 'end_date', 'temp_op_letter_issued', 'temp_op_letter_expiration', 'license_category', 'application_category', 'building_number', 'street', 'street_2', 'unit_type', 'unit', 'description', 'city', 'state', 'zip', 'contact_phone', 'longitude', 'latitude', 'active_vehicles']

        for df_val in range(len(df_list)):
            df = df_list[df_val]
            columns = df.columns.tolist()
            if columns == column_0:
                df_list[df_val] = self.apply_template(df,0)
            elif columns == column_1:
                df_list[df_val] = self.apply_template(df,1)
            elif columns == column_2:
                df_list[df_val] = self.apply_template(df,2)
            else:
                sys.exit('Columns do not match any templates.')
        return df_list

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df() 
        df_list = self.get_template(df_list) 
        return pd.concat(df_list, ignore_index=True)

    def instantiate_file(self):
        self.df = self.df.rename(columns={"Application ID": "Record ID 2", "License Number": "Record ID", "Expiration Date": "LIC Exp Date", "License Category": "Industry", "Start Date": "APP Start Date", "End Date": "APP End Date", "Status": "APP Status", "Status Date": "APP Status Date", "License Start": "LIC Start Date"})

        og = ['118','124','101','49','100','9','50','94','78','34','5','86','18','6','64','73','98','107','109','80','125','46','122','75','123','1','33','127','24','13','15','87','21','110','16','66','120','91','102','62','14']
        replace = ['Scrap Metal Processor - 118','Tow Truck Company - 124','Home Improvement Salesperson - 101','Amusement Device Temporary','Garage - 049','Home Improvement Contractor - 100','Parking Lot - 050','General Vendor - 094','Sightseeing Bus - 078','Employment Agency - 034','Secondhand Dealer Auto - 005','Horse Drawn Cab Driver - 086','Amusement Device (Portable) - 018','Secondhand Dealer [General] - 006','Laundry - 064','Cabaret - 073','Garage & Parking Lot - 098','Scale Dealer/Repairer - 107','Process Server (Organization) - 109','Pawnbroker - 080','Tow Truck Driver - 125','Pool or Billiard Room - 046','Debt Collection Agency - 122','Catering Establishment - 075','Motion Picture Projectionist - 123','Electronic Store - 001','Stoop Line Stand - 033','Cigarette Retail Dealer - 127','Newsstand - 024','Sidewalk Cafe - 013','Electronic & Home Appliance Service Dealer - 115','Horse Drawn Cab Owner - 087','Sightseeing Guide - 021','Process Server (Individual) - 110','Amusement Device (Permanent) - 016','Laundry Jobber - 066','Storage Warehouse - 120','Commercial Lessor (Bingo/Games Of Chance) - 091','Special Sale - 102','Locksmith - 062','Amusement Arcade - 014']
        industry_dict = {'3' : 'Tickets-Live Perf - 260','420':'Shoe Store', '53': 'Hardware Retail', '72' : 'Tickets-Live Perf - 260', '114': 'Auto Repair', '115': 'Electronic & Home Appliance Service Dealer', '119':'Dealer In Products For The Disabled', 'C73' : 'Event Space', 'E75':'CATERING ESTABLISHMENT', 'H27':'Barber', 'H29':'Pet Store', 'H92':'Healthcare', 'H90':'Healthcare', 'H98':'Healthcare', 'H99':'Healthcare', 'SD6':'Auto Parts'}
        
        self.df["Industry"] = self.df["Industry"].replace(og,replace)
        self.df["Industry"] = self.df["Industry"].replace(industry_dict.keys(),industry_dict.values())
        self.df['APP End Date'] = self.df['APP End Date'].astype('datetime64[D]')
        self.df['APP Start Date'] = self.df['APP Start Date'].astype('datetime64[D]')
        self.df['APP Status Date'] = self.df['APP Status Date'].astype('datetime64[D]')
        self.df['LIC Start Date'] = self.df['LIC Start Date'].astype('datetime64[D]')
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')
        self.df['APP Status'] = self.df['APP Status'].astype(str)
        self.df['License Type'] = self.df['License Type'].astype(str)
        self.df['Record ID 2'] = self.df['Record ID 2'].astype(str)
        self.df['Application or Renewal'] = self.df['Application or Renewal'].astype(str)
        self.df['Temp Op Letter Issued'] = self.df['Temp Op Letter Issued'].astype('datetime64[D]')
        self.df['Temp Op Letter Expiration'] = self.df['Temp Op Letter Expiration'].astype('datetime64[D]')
        self.df['Unit Type'] = self.df['Unit Type'].astype(str)
        self.df['Unit'] = self.df['Unit'].astype(str)
        self.df['Description'] = self.df['Description'].astype(str)
        self.df['Street 2'] = self.df['Street 2'].astype(str)

        del self.df["Application Category"]
        del self.df["Last Update Date"]
        
        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCAApplicationSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()
