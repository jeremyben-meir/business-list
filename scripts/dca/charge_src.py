#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DCAChargeSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['charge'],'charge')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 1:
            df.columns = ['Record ID', 'Certificate Number', 'Business Name', 'Violation Date', 'Industry', 'Borough', 'Charge', 'Charge Count', 'Outcome', 'Counts Settled', 'Counts Guilty', 'Counts Not Guilty', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Longitude', 'Latitude']
        return df

    def get_template(self,df_list):
        column_0 = ['Record ID', 'Certificate Number', 'Business Name', 'Violation Date', 'Industry', 'Borough', 'Charge', 'Charge Count', 'Outcome', 'Counts Settled', 'Counts Guilty', 'Counts Not Guilty', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Longitude', 'Latitude']
        column_1 = ['record_id', 'certificate_number', 'business_name', 'violation_date', 'industry', 'borough', 'charge', 'charge_count', 'outcome', 'counts_settled', 'counts_guilty', 'counts_not_guilty', 'building_number', 'street', 'street_2', 'unit_type', 'unit', 'description', 'city', 'state', 'zip', 'longitude', 'latitude']

        for df_val in range(len(df_list)):
            df = df_list[df_val]
            columns = df.columns.tolist()
            if columns == column_0:
                df_list[df_val] = self.apply_template(df,0)
            elif columns == column_1:
                df_list[df_val] = self.apply_template(df,1)
            else:
                sys.exit('Columns do not match any templates.')
        return df_list
    
    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df() 
        df_list = self.get_template(df_list) 
        return pd.concat(df_list, ignore_index=True).sample(300)

    def instantiate_file(self):
        self.df = self.df.rename(columns={"Violation Date": "CHRG Date"})

        industry_dict = {'3' : 'Tickets-Live Perf - 260','420':'Shoe Store', '53': 'Hardware Retail', '72' : 'Tickets-Live Perf - 260', '114': 'Auto Repair', '115': 'Electronic & Home Appliance Service Dealer', '119':'Dealer In Products For The Disabled', 'C73' : 'Event Space', 'E75':'CATERING ESTABLISHMENT', 'H27':'Barber', 'H29':'Pet Store', 'H92':'Healthcare', 'H90':'Healthcare', 'H98':'Healthcare', 'H99':'Healthcare', 'SD6':'Auto Parts'}

        self.df['Contact Phone'] = ""
        self.df["Industry"] = self.df["Industry"].replace(industry_dict.keys(),industry_dict.values())
        self.df['CHRG Date'] = self.df['CHRG Date'].astype('datetime64[D]')
        self.df['Street 2'] = self.df['Street 2'].astype(str)
        self.df['Unit Type'] = self.df['Unit Type'].astype(str)
        self.df['Unit'] = self.df['Unit'].astype(str)
        self.df['Description'] = self.df['Description'].astype(str)

        del self.df["Certificate Number"]
        del self.df["Borough"]
        del self.df["Charge"]
        del self.df["Charge Count"]
        del self.df["Outcome"]
        del self.df["Counts Settled"]
        del self.df["Counts Guilty"]
        del self.df["Counts Not Guilty"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCAChargeSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()
