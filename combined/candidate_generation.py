 # -*- coding: utf-8 -*- 
"""
@author: zamira niyazova
"""
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from textblob import TextBlob

import re, nltk
from nltk.corpus import wordnet
from nltk.chunk import RegexpParser
from nltk.corpus import wordnet as wn
from nltk.stem import WordNetLemmatizer

from ult.rake import Rake
from collections import Counter
from sematch.semantic.similarity import WordNetSimilarity



"""
================================================================================
 ###        Part-of-Speech tagging
 ###        feature generation
 ================================================================================        
"""

def tagged_sents(text):    
    sentences = nltk.sent_tokenize(text)
    sentences = [nltk.word_tokenize(sent) for sent in sentences]
    tagged_sentences = [nltk.pos_tag(sent) for sent in sentences]
    return tagged_sentences

def tagged_nouns(tagged_sentences):
    nouns = []
    for sentence in tagged_sentences:
        for (word, tag) in sentence:
            if tag.find('NN') != -1 or tag.find('NNS') != -1:
                if len(word)>2:
                    nouns.append(word)
    return nouns

def tagged_adjectives(tagged_sentences):
    adjectives = []
    for sentence in tagged_sentences:
        for (word, tag) in sentence:
            if tag.find('JJ') != -1:
                adjectives.append(word)
    return adjectives


def get_chunks(tagged_sentences):
    grammar = r"""
    CHUNK1:
        {<NN.*><.*>?<JJ.*>}  # Any Noun terminated with Any Adjective
    
    CHUNK2:
        {<NN.*|JJ.*><.*>?<NN.*>}  # Nouns or Adjectives, terminated with Nouns
    """
    cp = RegexpParser(grammar)
    nouns_chunks = []
    for sent in tagged_sentences:
        tree = cp.parse(sent)
        for subtree in tree.subtrees(filter = lambda t: t.label() in ['CHUNK1', 'CHUNK2']):
            #print(subtree)
            chunk=''
            for word, tag in subtree.leaves():
                if tag in ['NN', 'NNS']:
                    if(len(word)>2):
                        chunk=chunk+word+" "
            nouns_chunks.append(" ".join(chunk.split()))
    return nouns_chunks 

"""
================================================================================
 ### Pruning methods
================================================================================        
"""
def remove_duplicates(l):
    return list(set(l))

# extract keyphrases using Rake lib
def keywords_extraction(text):
    rake = Rake("resources/SmartStoplist.txt")
    text_b = TextBlob(text)
    cleaned_tokens = [x for x in text_b.words]
    cleaned = " ".join(cleaned_tokens)
    keywords = rake.run(cleaned)
    return keywords

# return #top_num high scored words using TFiDf
def most_important_words_tfidf(business, top_num):
    vect = TfidfVectorizer(stop_words='english')
    dtm = vect.fit_transform(business.bag_of_words)
    features = vect.get_feature_names()
    # iterate through each business
    for review_id, row_data in business.iterrows():
        review_text = unicode(business.bag_of_words[review_id], 'utf-8')

        word_scores = {}
        for word in TextBlob(review_text).words:
            word = word.lower()
            if word in features:
                word_scores[word] = dtm[review_id, features.index(word)]

        top_scores = sorted(word_scores.items(), key=lambda x: x[1], reverse=True)[:top_num]
        top_words = []
        for word, score in top_scores:
            #print word," = ",score
            top_words.append(word) 
        business.loc[review_id]["tdidf_words"] = top_words
    return top_words

#find the most common words
def most_frequent_words(list_of_words):
    number_of_words = 30
    counter = Counter()
    for word in list_of_words:
        counter[word] += 1
    most_common_words=counter.most_common(number_of_words)
    d = dict(most_common_words)
    return d.keys()

#prune keywords checking threshold (5 in this case) 
def important_keywords(keywords):
    keywords_list = []
    for pair_item in keywords:
        if pair_item[1]>5:
            keywords_list.append(pair_item[0])
    return keywords_list

#prune nouns using tfidf_scores
def prune_non_important(nouns,tfidf_words):
    cleaned_nouns =[]
    wnl = WordNetLemmatizer()
    for noun in nouns:
        synsets = wordnet.synsets(noun.strip())
        if len(synsets)>0:
            synset=synsets[0]
            arr = synset.lexname().split('.')
            if arr[1] not in ["person", "location","group", "cognition", "time", "state", "feeling", "quantity"] and arr[0] not in ["adj", "adv", "verb"]:
                if noun in tfidf_words:
                    cleaned_nouns.append(wnl.lemmatize(noun).encode("utf8"))
    return remove_duplicates(cleaned_nouns)

#prune nouns using keywords
def prune_with_keywords(nouns,keywords_list):
    cleaned_nouns =[]
    wnl = WordNetLemmatizer()
    for noun in nouns:
        if any(noun in string for string in keywords_list):
            cleaned_nouns.append(wnl.lemmatize(noun).encode("utf8"))
    return remove_duplicates(cleaned_nouns)

#prune nouns using similarity WordNetSimilarity
def prune_non_simillar(nouns,category):
    result = []
    wns = WordNetSimilarity()
    category = re.sub("[^A-Za-z]+"," ",category) 
    categories = category.lstrip()
    categories = categories.split(' ')
    for item in categories:
        if len(item)>2:
            word=re.sub("[^A-Za-z]+"," ",item) 
            word=word.replace(' ', '_') 
            base_form = wn.morphy(word.lower())
            if base_form:
                #print word, base_form
                for noun in nouns:
                    score = wns.word_similarity(base_form, noun, 'wpath') # "Computing Semantic Similarity of Concepts in Knowledge Graphs."
                    #score = relatednessESA(base_form, noun)
                    if score>0.2:
                        result.append(noun)
    return remove_duplicates(result)

# prune nouns using different methods
def prune_nouns(business):
    for business_id, row_data in business.iterrows():
        #category = business.categories[business_id]
        keywords_list = important_keywords(business.keywords[business_id])
        nouns = business.nouns[business_id]
        tdidf_words = business.tdidf_words[business_id]

        cleaned_nouns = prune_non_important(nouns, tdidf_words)
        #clean_nouns = prune_non_simillar(cleaned_nouns,category)
        #cleaned_nouns = prune_with_keywords(cleaned_nouns, keywords_list)

        business.loc[business_id]["cleaned_nouns"] = cleaned_nouns
    return cleaned_nouns

"""
================================================================================
 ### Main function
================================================================================        
"""
review_data = pd.read_csv('resources/preprocessed_results.csv')
print(review_data)

#merge by business_id, combine text
business = review_data
print business

business['poiid'] = review_data['poiid']
business['category'] = review_data['category']
business['yelp_rating'] = review_data['yelp_rating']
business = review_data.groupby(['business_id', 'name', 'poiid', 'category']).agg({'text':lambda x:' '.join(x), 'bag_of_words':lambda x:' '.join(x), 'yelp_rating': 'mean'})
#business = business.reset_index()

business['tdidf_words'] = "" # create new row
business['cleaned_nouns'] = "" # create new row

#most_important_words_tfidf(business, 30)
business['keywords'] = business['text'].apply(keywords_extraction)
business['tagged_sentences'] = business['text'].apply(tagged_sents)
business['nouns'] = business['tagged_sentences'].apply(tagged_nouns)
business['nouns_chunks'] = business['tagged_sentences'].apply(get_chunks)

prune_nouns(business)

business = business.reset_index()  #drop index
business.drop(['tdidf_words','tagged_sentences','text','bag_of_words'], axis=1, inplace=True) # remove columns
business.to_csv('resources/business_nouns.csv', encoding='utf-8', index=False) #save to file

