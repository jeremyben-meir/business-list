from global_vars import *

client = docker.from_env()

for year in YEAR_TEST:

    indicator = False

    # Look at NY business list for a given year
    readfile = open(LOCAL_LOCUS_PATH + "data/"+year+"/NYBusinessListMerged.csv", "r")

    # Create new NY business list with BBLs for a given year
    writefile = open(LOCAL_LOCUS_PATH + "data/"+year+"/parallel/NYBusinessList+BBL.csv", "w")

    # Create list of geosupport-unreadable addresses
    unreadable = open(LOCAL_LOCUS_PATH + "data/"+year+"/parallel/GSUnreadableBusinesses.txt", "w")

    line = readfile.readline()
    writefile.write(line.rstrip('\n') + ",bbl\n")
    writefile.flush()

    for line in readfile:
        if len(line)>1:
            address = line.split("\",\"")[1].split(" #")[0]
            zipCode = line.split("\",\"")[4]
            if len(address) > 1:
                try:
                    response = requests.get("http://localhost:8080/geoclient/v2/search.json?input="+address+" "+zipCode)
                    decoded = response.content.decode("utf-8")
                    bbl = decoded.split("\"bbl\":\"")[1].split("\",\"")[0]
                    writefile.write(line.rstrip('\n') + "," + bbl + "\n")
                    writefile.flush()
                except:
                    writefile.write(line.rstrip('\n') + ",N/A\n")
                    unreadable.write(address.rstrip('\n') + " " + zipCode.rstrip('\n') + '\n')
                    writefile.flush()
                    unreadable.flush()

    writefile.close()
    unreadable.close()