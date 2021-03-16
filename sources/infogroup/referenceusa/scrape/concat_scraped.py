import os
from global_vars import *

masterfile = open(LOCAL_LOCUS_PATH + "data/whole/ReferenceUSAScraped.csv", 'w')

directory = os.fsencode("C:/Users/jsbmm/Downloads")

entered = False
headerLine = ""
usedLines = []
count = 0

for entry in os.scandir(directory):
    pathname = entry.path.decode('utf-8')
    if ".csv" in pathname and entry.is_file():
        readfile = open(pathname, 'r')

        if entered:
            readfile.readline()

        for line in readfile:
            masterfile.write(line)

        entered = True
        readfile.close()
        masterfile.flush()
masterfile.close()