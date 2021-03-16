from global_vars import *

for year in YEAR_LIST:

    business_list_phone_path = LOCAL_LOCUS_PATH + "data/whole/ReferenceUSAScraped.csv"
    business_list_BBL_path = LOCAL_LOCUS_PATH + "data/"+year+"/sources/NYBusinessList.csv"
    business_list_BBL_phone_path = LOCAL_LOCUS_PATH + "data/"+year+"/NYBusinessListMerged.csv"

    df_phone = pd.read_csv(business_list_phone_path).drop_duplicates()
    df_harvard = pd.read_csv(business_list_BBL_path)

    df_phone_tokeep = ['IUSA Number', 'Version Year', 'Executive First Name', 'Executive Gender', 'Executive Last Name', 'Executive Title', 'Home Business', 'NAICS 1', 'NAICS 10', 'NAICS 2', 'NAICS 3', 'NAICS 4', 'NAICS 5', 'NAICS 6', 'NAICS 7', 'NAICS 8', 'NAICS 9', 'Phone Number Combined', 'Primary NAICS', 'Primary SIC Code', 'Record Type', 'SIC Code 1', 'SIC Code 1 Ad Size', 'SIC Code 1 Year Appeared', 'SIC Code 10', 'SIC Code 2', 'SIC Code 3', 'SIC Code 4', 'SIC Code 5', 'SIC Code 6', 'SIC Code 7', 'SIC Code 8', 'SIC Code 9', 'Years In Database']
    df_harvard_tokeep = ['ABI', 'Archive Version Year', 'Address Line 1', 'Address Type Indicator', 'Business Status Code', 'CBSA Code', 'CBSA Level', 'Census Block', 'Census Tract', 'City', 'Company', 'Company Holding Status', 'County Code', 'CSA Code', 'Employee Size (5) - Location', 'FIPS Code', 'IDCode', 'Industry Specific First Byte', 'Latitude', 'Location Employee Size Code', 'Location Sales Volume Code', 'Longitude', 'Match Code', 'Office Size Code', 'Parent Actual Employee Size', 'Parent Actual Sales Volume', 'Parent Employee Size Code', 'Parent Number', 'Parent Sales Volume Code', 'Population Code', 'Sales Volume (9) - Location', 'Site Number', 'State', 'Subsidiary Number', 'Year Established', 'Yellow Page Code', 'Zip4', 'ZipCode']

    df_phone['IUSA Number'] = df_phone['IUSA Number'].apply(lambda x: x.replace("-","")).astype(int)
    df_phone['Version Year'] = df_phone['Version Year'].astype(int)
    df_harvard['ABI'] = df_harvard['ABI'].astype(int)
    df_harvard['Archive Version Year'] = df_harvard['Archive Version Year'].astype(int)
    
    df = pd.merge(df_harvard[df_harvard_tokeep], df_phone[df_phone_tokeep], how='left', left_on=['ABI','Archive Version Year'], right_on=['IUSA Number','Version Year'])

    df.to_csv(business_list_BBL_phone_path, index=False, quoting=csv.QUOTE_NONNUMERIC)