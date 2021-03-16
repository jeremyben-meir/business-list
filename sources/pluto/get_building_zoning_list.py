from global_vars import *

client = docker.from_env()

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def containerStop(currentContainer):
    try:
        currentContainer.stop()
        currentContainer.wait(timeout=20)
    except:
        time.sleep(1)

def containerStart(containerName):
    newContainer = client.containers.get(containerName)
    newContainer.start()
    time.sleep(20)
    return newContainer

for year in YEAR_TEST:

    indicator = False

    pluto_file_path = LOCAL_LOCUS_PATH + "data/"+year+"/sources/NYPlutoList.csv"
    building_sample_file_path = LOCAL_LOCUS_PATH + "data/"+year+"/parallel/BuildingsSample.json"
    building_sample_csvfile_path = LOCAL_LOCUS_PATH + "data/"+year+"/parallel/BuildingsSample.csv"
    building_zoning_file_path = LOCAL_LOCUS_PATH + "data/"+year+"/buildingsAndZoning.csv"

    n = sum(1 for line in open(pluto_file_path)) - 1 # number of records in file (excludes header)
    s = 10000 # desired sample size
    skip = sorted(random.sample(range(1,n+1),n-s)) # the 0-indexed header will not be included in the skip list
    df_pluto = pd.read_csv(pluto_file_path, skiprows=skip) 

    building_sample_file = open(building_sample_file_path, "w")
    building_sample_file.write("{\"bbl\": [")
    building_sample_file.flush()

    for index, row in df_pluto.iterrows():
        try:
            if int(row['retailarea']) and int(row['garagearea']):
                response = requests.get("http://localhost:8080/geoclient/v2/bbl.json?borough="+str(row['borough'])+"&block="+str(row['block'])+"&lot="+str(row['lot']))
                decoded = response.content.decode("utf-8")
                if indicator:
                    building_sample_file.write(",")
                indicator = True
                augmentedcut = decoded[8:len(decoded)-1]
                building_sample_file.write("{\"year\":\""+year+"\",")
                building_sample_file.write(augmentedcut)
                building_sample_file.flush()
        except:
            response = requests.get("http://localhost:8080/geoclient/v2/bbl.json?borough="+str(row['borough'])+"&block="+str(row['block'])+"&lot="+str(row['lot']))
            decoded = response.content.decode("utf-8")
            if indicator:
                building_sample_file.write(",")
            indicator = True
            augmentedcut = decoded[8:len(decoded)-1]
            building_sample_file.write("{\"year\":\""+year+"\",")
            building_sample_file.write(augmentedcut)
            building_sample_file.flush()


    building_sample_file.write("]}")
    building_sample_file.flush()
    building_sample_file.close

    # TURN THE JSON FILE INTO BUILDING/ZONING AND SAVE TO COMPUTER

    inputFile = open(building_sample_file_path, "r")

    json_data = json.loads(inputFile.read())
    df_building_sample = pd.DataFrame([flatten_json(elt)for elt in json_data['bbl']]).applymap

    df_building_sample['bbl'] = df_building_sample['bbl'].astype(int)
    df_pluto['bbl'] = df_pluto['bbl'].astype(int)

    df = pd.merge(df_building_sample, df_pluto, how='left', left_on=['bbl'], right_on=['bbl'])
    df.to_csv(building_zoning_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)