#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DCALicenseSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['license','revocation'], 'license')
        super().__init__(self.retrieve_file(file_manager),file_manager)

    def apply_template(self, df, template):
        if template == 0:
            df = df.rename(columns={"license_nbr": "Record ID",'license_type':'LIC Type','lic_expir_dd':'LIC Exp Date','license_status':'LIC Status','license_creation_date':'LIC Start Date','industry':'Industry','business_name':'Business Name','business_name_2':'Business Name 2', 'address_building':'Building Number', 'address_street_name':'Street', 'address_city':'City', 'address_state':'State','address_zip':'Zip', 'contact_phone':'Contact Phone', 'bbl':'BBL', 'longitude':'Longitude', 'latitude':'Latitude'})

        if template == 1:
            df = df.rename(columns={'dca_license_number':'Record ID','business_name':'Business Name','business_name_2':'Business Name 2','industry':'Industry','event_type':'RSS', 'event_date':'RSS Date','status':'LIC Status'})
            
            df["RSS Date"] = df["RSS Date"].astype('datetime64[D]')
            df["RSS"] = df["RSS"].astype(str)
            df = df.loc[df.reset_index().groupby(['Record ID'])['RSS Date'].idxmax()]
            df = df[~df["RSS"].str.contains("Reinstated", na=False)]

            df = df[['Record ID', 'RSS', 'RSS Date']]
        
        return df

    def get_template(self,df_list):
        column_0 = ['license_nbr', 'license_type', 'lic_expir_dd', 'license_status', 'license_creation_date', 'industry', 'business_name', 'business_name_2', 'address_building', 'address_street_name', 'address_street_name_2', 'address_city', 'address_state', 'address_zip', 'contact_phone', 'address_borough', 'detail', 'community_board', 'council_district', 'bin', 'bbl', 'nta', 'census_tract', 'detail_2', 'longitude', 'latitude', 'location']
        column_1 = ['dca_license_number', 'business_name', 'business_name_2', 'industry', 'event_type', 'event_date', 'status']
        
        df_lic = pd.DataFrame()
        df_rev = pd.DataFrame()

        for df_val in range(len(df_list)):
            df = df_list[df_val]
            columns = df.columns.tolist()
            if columns == column_0:
                df = self.apply_template(df,0)
                df_lic = pd.concat([df_lic,df], ignore_index=True)
                df_lic = df_lic.drop_duplicates()
            elif columns == column_1:
                df = self.apply_template(df,1)
                df_rev = pd.concat([df_rev,df], ignore_index=True)
                df_rev = df_rev.drop_duplicates()
            else:
                sys.exit('Columns do not match any templates.')
        
        df = pd.merge(df_lic, df_rev, how = 'left', on = ['Record ID'])
        df_list = [df]

        return df_list

    def retrieve_file(self,file_manager):
        df_list = file_manager.retrieve_df() 
        df_list = self.get_template(df_list) 
        return pd.concat(df_list, ignore_index=True).sample(30000)

    def instantiate_file(self):
        industry_dict = {'3' : 'Tickets-Live Perf - 260','420':'Shoe Store', '53': 'Hardware Retail', '72' : 'Tickets-Live Perf - 260', '114': 'Auto Repair', '115': 'Electronic & Home Appliance Service Dealer', '119':'Dealer In Products For The Disabled', 'C73' : 'Event Space', 'E75':'CATERING ESTABLISHMENT', 'H27':'Barber', 'H29':'Pet Store', 'H92':'Healthcare', 'H90':'Healthcare', 'H98':'Healthcare', 'H99':'Healthcare', 'SD6':'Auto Parts'}

        self.df['LIC Type'] = self.df['LIC Type'].astype(str)
        self.df["Industry"] = self.df["Industry"].replace(industry_dict.keys(),industry_dict.values())
        self.df['LIC Exp Date'] = self.df['LIC Exp Date'].astype('datetime64[D]')
        self.df['LIC Status'] = self.df['LIC Status'].astype(str)
        self.df['LIC Start Date'] = self.df['LIC Start Date'].astype('datetime64[D]')
        self.df['RSS'] = self.df['RSS'].astype(str)
        self.df['RSS Date'] = self.df['RSS Date'].astype('datetime64[D]')

        del self.df['address_street_name_2']
        del self.df['address_borough']
        del self.df['detail']
        del self.df['community_board']
        del self.df['council_district']
        del self.df['bin']
        del self.df['nta']
        del self.df['census_tract']
        del self.df['detail_2']
        del self.df['location']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCALicenseSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()