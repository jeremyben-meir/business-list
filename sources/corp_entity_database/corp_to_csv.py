from global_vars import *

# my_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/df-"+str(0)+".p","rb"))

# my_df.to_csv(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/test.csv",index=False,quoting=csv.QUOTE_ALL)

my_df = pickle.load(open(LOCAL_LOCUS_PATH + "data/whole/corp_entity_database/charlist-"+str(0)+".p","rb"))
print(my_df)