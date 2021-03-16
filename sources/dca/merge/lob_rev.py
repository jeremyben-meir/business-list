from global_vars import *

def get_years_to_decuct_from_industry(industry):
    dictionary = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/industry_expirations.p","rb"))
    val = dictionary[industry.lower()]
    if val == 3 or val == 2 or val == 1:
        return 2
    return 1

def fix_bbl(row):

    try:
        row['Address City'] = correct_nyc_city[nyc_city.index(row['Address City'])] if row['Address City'] != "" else ""
    except:
        row['Address City'] = ""

    if (row["BBL Latest"]=="" or row["BBL Latest"]=="nan" or pd.isna(row["BBL Latest"]) or pd.isnull(row["BBL Latest"])):
        row["BBL Latest"]=row["BBL"]

    return row

def add_bbl(row):

    year = str(row['License Creation Date'].year)[-1]
    last = ('3' if int(year) <= 3 else '5' if int(year) <= 5 else year)

    try:
        row['Address City'] = correct_nyc_city[nyc_city.index(row['Address City'])] if row['Address City'] != "" else ""
    except:
        row['Address City'] = ""

    entry = (str(row['Address Building']) + " " + str(row['Address Street Name']) + " " + str(row['Address City']) if not row['Address ZIP'].isdigit() else str(row['Address Building']) + " " + str(row['Address Street Name']) + " " + str(row['Address ZIP']))

    try:
        response = requests.get("http://localhost:808"+last+"/geoclient/v2/search.json?input="+ entry)
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        row["BBL"]=json_loaded['results'][0]['response']['bbl']
    except:
        row["BBL"]=""

    try:
        response = requests.get("http://localhost:8089/geoclient/v2/search.json?input="+ entry)
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        row["BBL Latest"]=json_loaded['results'][0]['response']['bbl']
    except:
        row["BBL Latest"]=""

    return row

def format_process():
    revocation_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/revocations_updated_0.csv"
    lob_file_path = LOCAL_LOCUS_PATH + "data/whole/dca_files/source/Legally_Operating_Businesses.csv"

    rev_df = pd.read_csv(revocation_file_path)
    lob_df = pd.read_csv(lob_file_path)

    del rev_df["Status"]

    merged = pd.merge(lob_df,rev_df,how="left", left_on="DCA License Number", right_on="DCA License Number")

    merged["Business Name"] = merged[["Business Name_x","Business Name_y","Business Name 2_x","Business Name 2_y"]].values.tolist()
    merged["Industry"] = merged[["Industry_x","Industry_y"]].values.tolist()

    merged["Business Name"] = merged["Business Name"].apply(lambda x:  list(set([y for y in x if not pd.isna(y)])))
    merged["Industry"] = merged["Industry"].apply(lambda x: list(set([y for y in x if not pd.isna(y)])))

    del merged["Business Name_x"]
    del merged["Business Name_y"]
    del merged["Business Name 2_x"]
    del merged["Business Name 2_y"]
    del merged["Industry_x"]
    del merged["Industry_y"]

    # dictionary = {}
    # for x in open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/industry_expirations.csv","r"):
    #     dictionary[x.split(',')[0].lower()] = int(x.split(',')[1])
    # print(dictionary)
    # pickle.dump(dictionary, open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/industry_expirations.p","wb"))

    merged = merged.rename(columns={"Event Type": "RSS", "Event Date": "RSS Date"})

    merged = merged[merged["Address Borough"] != "Outside NYC"]

    merged["BBL"] = merged["BBL"].astype(str)
    merged["BBL Latest"] = merged["BBL"]
    merged["Address ZIP"] = merged["Address ZIP"].astype(str)
    merged["Address City"] = merged["Address City"].str.upper()
    merged["License Creation Date"] = merged["License Creation Date"].astype('datetime64[D]')
    merged['License Expiration Date'] = merged['License Expiration Date'].astype('datetime64[D]')

    merged["Contact Phone Number"] = merged["Contact Phone Number"].astype(str).replace("EXT","").replace("nan","").replace("-","")
    merged["Contact Phone Number"] = merged["Contact Phone Number"].apply(lambda x: str(int(float(x))) if x != "" and x.isdigit() and not math.isnan(float(x)) else np.nan).astype(str)
    merged["Contact Phone Number"] = merged["Contact Phone Number"].apply(lambda x: np.nan if x == "" or x == "nan" else x)

    merged = merged.apply(lambda x: add_bbl(x) if (x["BBL"]=="" or x["BBL"]=="nan" or pd.isna(x["BBL"]) or pd.isnull(x["BBL"])) and not (x["Address Building"]=="" or x["Address Building"]=="nan" or pd.isna(x["Address Building"]) or pd.isnull(x["Address Building"])) else x, axis = 1)
    merged = merged.apply(lambda x: fix_bbl(x), axis = 1)

    return merged

def begin_process():

    df = format_process()
    pickle.dump(df,open(LOCAL_LOCUS_PATH + "data/whole/dca_files/temp/lob_revocations.p","wb"))
    df.to_csv(LOCAL_LOCUS_PATH + "data/whole/dca_files/lob_revocations.csv",index=False,quoting=csv.QUOTE_ALL)

if __name__ == "__main__":
    begin_process()