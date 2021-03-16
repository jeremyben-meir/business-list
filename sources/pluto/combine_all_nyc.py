from global_vars import *

client = docker.from_env()

for year in YEAR_TEST:
    # newContainer = client.containers.get(year)
    # newContainer.start()
    # time.sleep(20)

    building_zoning_list_path = LOCAL_LOCUS_PATH + "data/"+year+"/buildingsAndZoning.csv"
    full_business_list_path = LOCAL_LOCUS_PATH + "data/"+year+"/parallel/NYBusinessList+BBL.csv"
    BBZ_file_path = LOCAL_LOCUS_PATH + "data/"+year+"/BBZ.csv"

    df_building_zoning = pd.read_csv(building_zoning_list_path)
    df_business_list = pd.read_csv(full_business_list_path)

    df_building_zoning['bbl'] = df_building_zoning['bbl'].astype(int)
    df_business_list['bbl'] = df_business_list['bbl'].astype(int)

    df = pd.merge(df_building_zoning, df_business_list, how='left', left_on=['bbl'], right_on=['bbl'])
    df.to_csv(BBZ_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    # try:
    #     currentContainer.stop()
    #     currentContainer.wait(timeout=20)
    # except:
    #     time.sleep(1)
