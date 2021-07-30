#######IMPORTS#######

from scripts.file_manager import FileManager
from scripts.source_file import SourceFile, pd, sys

#######FUNCTION DEFINITIONS#########

class DOHInspectionSrcFile(SourceFile):

    def __init__(self):
        file_manager = FileManager('doh',['inspection'], 'inspection')
        super().__init__(self.retrieve_file(file_manager), file_manager)

    def apply_template(self, df, template):
        if template == 0:
            df = df.rename(columns={"CAMIS": "Record ID", 'DBA':'Business Name', 'BORO':'City', 'ZIPCODE':'Zip','PHONE':'Contact Phone', 'CUISINECODE':'Industry','INSPDATE':'INSP Date','CASE_DECISION_DATE':'Case Dec. Date','CURRENTGRADE':'Grade','GRADEDATE':'Grade Date'})
        
            df['City'] = df['City'].replace(['0','1','2','3',"4","5"],['','NEW YORK', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND'])
            df['Building Number'] = df['ADDRESS'].apply(lambda x: x.split(" ")[0])
            df['Street'] = df['ADDRESS'].apply(lambda x: " ".join(x.split(" ")[1:]))

            ind_dict = {'00':'', '01':'Afghan', '02':'African', '03':'American', '04':'Armenian', '05':'Asian', '06':'Australian', '07':'Bagels/Pretzels', '08':'Bakery', '09':'Bangladeshi', '10':'Barbecue', '11':'Basque', '12':'Bottled beverages, including water, sodas, juices, etc.', '13':'Brazilian', '14':'Café/Coffee/Tea', '15':'Cajun', '16':'Californian', '17':'Caribbean', '18':'Chicken', '19':'Chilean', '20':'Chinese', '21':'Chinese/Cuban', '22':'Chinese/Japanese', '23':'Continental', '24':'Creole', '25':'Creole/Cajun', '26':'Czech', '27':'Delicatessen', '28':'Vietnamese/Cambodian/Malaysia', '29':'Donuts', '30':'Eastern European', '31':'Egyptian', '32':'English', '33':'Ethiopian', '34':'Filipino', '35':'French', '36':'Fruits/Vegetables', '37':'German', '38':'Greek', '39':'Hamburgers', '40':'Hawaiian', '41':'Hotdogs', '42':'Hotdogs/Pretzels', '43':'Ice Cream, Gelato, Yogurt, Ices', '44':'Indian', '45':'Indonesian', '46':'Iranian', '47':'Irish', '48':'Italian', '49':'Japanese', '50':'Jewish/Kosher', '51':'Juice, Smoothies, Fruit Salads', '52':'Korean', '53':'Latin (Cuban, Dominican, Puerto Rican, South & Central American)', '54':'Mediterranean', '55':'Mexican', '56':'Middle Eastern', '57':'Moroccan', '58':'Nuts/Confectionary', '59':'Pakistani', '60':'Pancakes/Waffles', '61':'Peruvian', '62':'Pizza', '63':'Pizza/Italian', '64':'Polish', '65':'Polynesian', '66':'Portuguese', '67':'Russian', '68':'Salads', '69':'Sandwiches', '70':'Sandwiches/Salads/Mixed Buffet', '71':'Scandinavian', '72':'Seafood', '73':'Soul Food', '74':'Soups', '75':'Soups & Sandwiches', '76':'Southwestern', '77':'Spanish', '78':'Steak', '80':'Tapas', '81':'Tex-Mex', '82':'Thai', '83':'Turkish', '84':'Vegetarian', '99':'Other'}
            
            df['Industry'] = df['Industry'].astype(str)
            def replace_key(x):
                for key in ind_dict:
                    x=x.replace(key,ind_dict[key])
                return x
            df['Industry'] = df['Industry'].apply(lambda x: replace_key(x))
            df['Industry'] = df['Industry'].apply(lambda x: "Restaurant - "+x)
            df['Industry'] = df['Industry'].apply(lambda x: x.strip('        '))

        elif template == 1:
            df = df.rename(columns={"camis": "Record ID", 'dba':'Business Name', 'boro':'City', 'building': 'Building Number', 'street':'Street', 'zipcode':'Zip','phone':'Contact Phone', 'cuisine_description':'Industry','inspection_date':'INSP Date','grade':'Grade','grade_date':'Grade Date','inspection_type':'INSP Type','bbl':'BBL'})
            df['Industry'] = df['Industry'].astype(str)
            df['Industry'] = df['Industry'].replace(['N/A','NULL','nan','NaN'],'')
            df['Industry'] = df['Industry'].apply(lambda x: "Restaurant-"+x)
        
        return df

    def get_template(self,df_list):
        column_0 = ['CAMIS', 'DBA', 'BORO', 'ADDRESS', 'ZIPCODE', 'PHONE', 'CUISINECODE', 'INSPDATE', 'ACTION', 'PROGRAM', 'INSPTYPE', 'VIOLCODE', 'DISMISSED', 'SCORE', 'VIOLSCORE', 'CASE_DECISION_DATE', 'MOD_TOTALSCORE', 'CURRENTGRADE', 'GRADEDATE']
        column_1 = ['camis', 'dba', 'boro', 'building', 'street', 'zipcode', 'phone', 'cuisine_description', 'inspection_date', 'action', 'violation_code', 'violation_description', 'critical_flag', 'score', 'grade', 'grade_date', 'record_date', 'inspection_type', 'latitude', 'longitude', 'community_board', 'council_district', 'census_tract', 'bin', 'bbl', 'nta']

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
        return pd.concat(df_list, ignore_index=True)

    def instantiate_file(self):

        self.df['State'] = 'NY'
        self.df['INSP Date'] = self.df['INSP Date'].astype('datetime64[D]')
        self.df['INSP Type'] = self.df['INSP Type'].astype(str)
        self.df['Case Dec. Date'] = self.df['Case Dec. Date'].replace(['N/A','NULL','nan'],'')
        self.df['Case Dec. Date'] = self.df['Case Dec. Date'].astype('datetime64[D]')
        self.df['Grade'] = self.df['Grade'].astype(str)
        self.df['Grade Date'] = self.df['Grade Date'].replace(['N/A','NULL','nan'],'')
        self.df['Grade Date'] = self.df['Grade Date'].astype('datetime64[D]')

        del self.df['ADDRESS']
        del self.df['ACTION']
        del self.df['PROGRAM']
        del self.df['INSPTYPE']
        del self.df['VIOLCODE']
        del self.df['DISMISSED']
        del self.df['SCORE']
        del self.df['VIOLSCORE']
        del self.df['MOD_TOTALSCORE']
        del self.df['violation_code']
        del self.df['violation_description']
        del self.df['critical_flag']
        del self.df['action']
        del self.df['score']
        del self.df['latitude']
        del self.df['longitude']
        del self.df['community_board']
        del self.df['council_district']
        del self.df['census_tract']
        del self.df['bin']
        del self.df['nta']
        del self.df['record_date']

        self.type_cast()
        self.clean_zip_city()
        self.df = self.df.drop_duplicates()

        self.file_manager.store_pickle(self.df,0)
        
if __name__ == '__main__':
    source = DOHInspectionSrcFile()
    source.instantiate_file()
    source.add_bbl_async(overwrite=False)
    source.save_csv()