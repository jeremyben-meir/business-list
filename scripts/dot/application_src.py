#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys
from datetime import datetime
import math

#######FUNCTION DEFINITIONS#########

class DOTApplicationSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dot',['application'], 'application')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 0:
            pass
        return df

    def get_template(self,df_list):
        column_0 = ['objectid', 'globalid', 'seating_interest_sidewalk', 'restaurant_name', 'legal_business_name', 'doing_business_as_dba', 'bulding_number', 'street', 'borough', 'zip', 'business_address', 'food_service_establishment', 'sidewalk_dimensions_length', 'sidewalk_dimensions_width', 'sidewalk_dimensions_area', 'roadway_dimensions_length', 'roadway_dimensions_width', 'roadway_dimensions_area', 'approved_for_sidewalk_seating', 'approved_for_roadway_seating', 'qualify_alcohol', 'sla_serial_number', 'sla_license_type', 'landmark_district_or_building', 'landmarkdistrict_terms', 'healthcompliance_terms', 'time_of_submission', 'latitude', 'longitude', 'community_board', 'council_district', 'census_tract', 'bin', 'bbl', 'nta']

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

        self.df = self.df.rename(columns={ 'globalid':'Record ID', 'restaurant_name':'Business Name', 'legal_business_name':'Business Name 2', 'bulding_number':'Building Number', 'street':'Street', 'borough':'City', 'zip':'Zip', 'food_service_establishment':'Record ID 2', 'approved_for_sidewalk_seating':'Sidewalk Seating Flag', 'approved_for_roadway_seating':'Roadway Seating Flag', 'sla_serial_number':'Record ID 3', 'time_of_submission':'APP Start Date', 'bbl':'BBL', 'longitude':'Longitude', 'latitude':'Latitude'})

        self.df['State'] = 'NY'
        self.df['Contact Phone'] = ''
        self.df['Industry'] = 'Restaurant'
        self.df['Record ID'] = self.df['Record ID'].apply(lambda x: str(x).replace('{','').replace('}',''))
        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        self.df['Record ID 2'] = self.df['Record ID 2'].astype(str)
        self.df['Business Name 2'] = self.df['Business Name 2'].astype(str)
        self.df['Sidewalk Seating Flag'] = self.df['Sidewalk Seating Flag'].replace(['yes','no'],['1','0'])
        self.df['Roadway Seating Flag'] = self.df['Roadway Seating Flag'].replace(['yes','no'],['1','0'])
        self.df['Record ID 3'] = self.df['Record ID 3'].apply(lambda x: str(x).split(';')[0] if str(x).split(';')[0].isdigit() else  '')
        self.df['APP Start Date'] = self.df['APP Start Date'].astype('datetime64[D]')

        del self.df['objectid']
        del self.df['seating_interest_sidewalk']
        del self.df['doing_business_as_dba']
        del self.df['business_address']
        del self.df['sidewalk_dimensions_length']
        del self.df['sidewalk_dimensions_width']
        del self.df['sidewalk_dimensions_area']
        del self.df['roadway_dimensions_length']
        del self.df['roadway_dimensions_width']
        del self.df['roadway_dimensions_area']
        del self.df['qualify_alcohol']
        del self.df['sla_license_type']
        del self.df['landmark_district_or_building']
        del self.df['landmarkdistrict_terms']
        del self.df['healthcompliance_terms'] 
        del self.df['community_board']
        del self.df['council_district']
        del self.df['census_tract']
        del self.df['bin']
        del self.df['nta']
        
        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOTApplicationSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()