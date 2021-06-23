#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd
import sys

#######FUNCTION DEFINITIONS#########

class DCAInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('dca',['inspections'], 'inspections')
        super().__init__(self.retrieve_file(file_manager), file_manager)
    
    def apply_template(self, df, template):
        if template == 0:
            df.columns = ['Record ID', 'Certificate Number', 'Business Name', 'Inspection Date', 'Inspection Result', 'Industry', 'Borough', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Longitude', 'Latitude']
        if template == 1:
            df.columns = ['Record ID', 'Certificate Number', 'Business Name', 'Inspection Date', 'Inspection Result', 'Industry', 'Borough', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Longitude', 'Latitude']
        if template == 2:
            df.columns = ['Record ID', 'Certificate Number', 'Business Name', 'Inspection Date', 'Inspection Result', 'Industry', 'Borough', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Longitude', 'Latitude']
        return df

    def get_template(self,df_list):
        column_0 = ['REC_ID', 'CERT_NBR', 'BIZ_NAME', 'INSP_DT', 'INSP_RSLT', 'INDUSTRY', 'BORO', 'BLDG_NBR', 'STREET', 'STREET2', 'UNIT_TYP', 'UNIT', 'DESCR', 'CITY', 'STATE', 'ZIP', 'X_COORD', 'Y_COORD']
        column_1 = ['record_id', 'certificate_number', 'business_name', 'inspection_date', 'inspection_result', 'industry', 'borough', 'building_number', 'street', 'street_2', 'unit_type', 'unit', 'description', 'city', 'state', 'zip', 'longitude', 'latitude']
        column_2 = ['Record ID', 'Certificate Number', 'Business Name', 'Inspection Date', 'Inspection Result', 'Industry', 'Borough', 'Building Number', 'Street', 'Street 2', 'Unit Type', 'Unit', 'Description', 'City', 'State', 'Zip', 'Longitude', 'Latitude']

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
        self.df = self.df.rename(columns={"Inspection Date": "INSP Date", 'Inspection Result':"INSP Result"})

        business_indicators_noninformative = ["Business Padlocked", "Unable to Locate", "Unable to Complete Inspection", "Unable to Seize Vehicle"]

        self.df['Contact Phone'] = ""
        self.df["INSP Result"] = self.df["INSP Result"].astype(str)
        self.df = self.df[~self.df["INSP Result"].isin(business_indicators_noninformative)]
        self.df["INSP Date"] = self.df["INSP Date"].astype('datetime64[D]')
        self.df['Street 2'] = self.df['Street 2'].astype(str)
        self.df['Unit Type'] = self.df['Unit Type'].astype(str)
        self.df['Unit'] = self.df['Unit'].astype(str)
        self.df['Description'] = self.df['Description'].astype(str)

        del self.df["Certificate Number"]
        del self.df["Borough"]
        del self.df["Longitude"]
        del self.df["Latitude"]

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DCAInspectionSrcFile()
    source.instantiate_file()
    source.add_bbl_async()
    source.save_csv()
