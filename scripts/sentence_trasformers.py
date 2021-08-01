from sentence_transformers import SentenceTransformer, util
import pandas as pd
import os
import csv
import pickle
import time

def industry_assign(inp_list):
    model_name = 'paraphrase-MiniLM-L6-v2'
    model = SentenceTransformer(model_name)

    try:
        naics = pickle.load(open("/Users/iggy1212 1/Dropbox/locus/data/temp/naics.p", "rb" ))
        corpus_embeddings = pickle.load(open("/Users/iggy1212 1/Dropbox/locus/data/temp/naics_encodings.p", "rb" ))

    except:
        naics = pd.read_csv(f'/Users/iggy1212 1/Dropbox/locus/data/2017_NAICS_Descriptions.csv')
        naics = naics[naics['Code'].str.len() == 6].reset_index(drop=False)
        corpus_sentences = list(naics['Description'])
        corpus_embeddings = model.encode(corpus_sentences, show_progress_bar=True, convert_to_tensor=True)

        pickle.dump(naics , open("/Users/iggy1212 1/Dropbox/locus/data/temp/naics.p", "wb" ))
        pickle.dump(corpus_embeddings , open("/Users/iggy1212 1/Dropbox/locus/data/temp/naics_encodings.p", "wb" ))

    inp_list = list(set(inp_list))
    inp = ''
    for i in inp_list:
        inp+=f'{i} '
    
    question_embedding = model.encode(inp, convert_to_tensor=True)
    hits = util.semantic_search(question_embedding, corpus_embeddings)
    return naics.iloc[hits[0][0]['corpus_id']]

print(industry_assign(['Cattle farmer','Cattle farmer','Soy farmer','Milk store']))

    