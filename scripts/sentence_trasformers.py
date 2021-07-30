from sentence_transformers import SentenceTransformer, util
import pandas as pd
import os
import csv
import pickle
import time

model_name = 'paraphrase-MiniLM-L6-v2'
model = SentenceTransformer(model_name)

max_corpus_size = 100000
naics = pd.read_csv('/Users/iggy1212 1/Dropbox/locus/data/2017_NAICS_Descriptions.csv')
naics = naics[naics['Code'].str.len() == 6].reset_index(drop=False)
corpus_sentences = list(naics['Description'])

print("Encode the corpus. This might take a while")
corpus_embeddings = model.encode(corpus_sentences, show_progress_bar=True, convert_to_tensor=True)

###############################
print("Corpus loaded with {} sentences / embeddings".format(len(corpus_sentences)))

#Move embeddings to the target device of the model
corpus_embeddings = corpus_embeddings.to(model._target_device)

while True:
    inp_question = input("Please enter a question: ")

    start_time = time.time()
    question_embedding = model.encode(inp_question, convert_to_tensor=True)
    hits = util.semantic_search(question_embedding, corpus_embeddings)
    end_time = time.time()
    hits = hits[0]  #Get the hits for the first query

    print("Input question:", inp_question)
    print("Results (after {:.3f} seconds):".format(end_time-start_time))
    for hit in hits[0:5]:
        print("\t{:.3f}\t{}".format(hit['score'], corpus_sentences[hit['corpus_id']]))

    print("\n\n========\n")

    