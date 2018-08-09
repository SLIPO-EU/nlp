# -*- coding: utf-8 -*-
"""
@author: zamira niyazova
"""
from __future__ import unicode_literals
import sys

reload(sys)
sys.setdefaultencoding("utf-8")
import os
import pandas as pd
import string
# import enchant
import re, nltk
from nltk.stem import SnowballStemmer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.corpus import wordnet
import multiprocessing as mp
from autocorrect import spell
import shutil

READ_BUFFER = 2 ** 13

''' 
================================================================================
#### Step 1  -  Text Cleaning  
    1) remove linebreaks and extra spaces in a text
    2) replace contraction won't ==> will not
    3) remove repeating characters until a valid word from dictionary is found.
    4) spelling correction using enchant dictionary

For cleaning run textCleaning(text) 
================================================================================
'''


def lineBreaksCleaner(text):
    s = text
    s = s.replace('\r\n', ' ')
    s = s.replace('\n\n', ' ')
    s = s.replace('\n', ' ')
    s = s.replace('\r', ' ')
    s = re.sub('\s\s+', ' ', s)  # extra spaces
    return s


# Replaces regular expression in a text.
def contractionReplacer(text):
    replacement_patterns = [
        (r'won\'t', 'will not'),
        (r'can\'t', 'cannot'),
        (r'i\'m', 'i am'),
        (r'I\'m', 'I am'),
        (r'ain\'t', 'is not'),
        (r'(\w+)\'ll', '\g<1> will'),
        (r'(\w+)n\'t', '\g<1> not'),
        (r'(\w+)\'ve', '\g<1> have'),
        (r'(\w+)\'s', '\g<1> is'),
        (r'(\w+)\'re', '\g<1> are'),
        (r'(\w+)\'d', '\g<1> would'),
    ]
    patterns = [(re.compile(regex), repl) for (regex, repl) in replacement_patterns]
    s = text
    for (pattern, repl) in patterns:
        s = re.sub(pattern, repl, s)

    return s


def repeatReplacer(word):
    # Removes repeating characters until a valid word from disctionary is found.
    repeat_regexp = re.compile(r'(\w*)(\w)\2(\w*)')
    repl = r'\1\2\3'
    repl_word = repeat_regexp.sub(repl, word)
    if wordnet.synsets(word):
        return word

    if repl_word != word:
        return repeatReplacer(repl_word)
    else:
        return repl_word


def spellingCorrection(word):  # enchant does not work on windows systems - exchanged it for the spell library which autocorrects each word
    # spelling correction using enchant dictionary
    dict_name = 'en'
    max_dist = 1
    # spell_dict = enchant.Dict(dict_name)
    # max_dist = max_dist
    # if spell_dict.check(word):
    #    return word
    # suggestions = spell_dict.suggest(word)
    # if suggestions and edit_distance(word, suggestions[0]) <= max_dist:
    #    return suggestions[0]
    # else:
    return spell(word)


"""
function for text cleaning 
split text in sentences 
for each word run spellingCorrection and repeatReplacer
@return cleaned text
"""


def textCleaning(text):
    text = lineBreaksCleaner(text)  # replaces \r\n expressions and extra spaces in a text
    text = contractionReplacer(text)  # replaces regular expression in a text.

    sentences = nltk.sent_tokenize(text.decode().encode('utf-8'))
    sentences = [nltk.word_tokenize(sent) for sent in sentences]

    cleaned_sentences = []
    for sent in sentences:
        corrected_words = []
        for word in sent:
            if word in string.punctuation:  # save punctuation
                corrected_words.append(word)
            else:
                word = spellingCorrection(word)
                word = repeatReplacer(word)
                if len(word) > 2:
                    corrected_words.append(' ' + word)
        corrected_words = "".join(corrected_words)
        cleaned_sentences.append(corrected_words.strip())
    return " ".join(cleaned_sentences)


'''
================================================================================
 #### Step 2  -  Text Preprocessing
    1) remove numbers, punctuation
    2) tokenize
    3) stop removing
    4) stemming
================================================================================
'''

def textPrepocessing(raw_text):
    stemmer = SnowballStemmer('english')
    wnl = WordNetLemmatizer()
    stops = set(stopwords.words("english"))

    only_text = re.sub("[^a-zA-Z]", " ", raw_text)  # Remove non-letters
    nltk_words = [word for sent in nltk.sent_tokenize(only_text.lower()) for word in
                  nltk.word_tokenize(sent)]  # tokenize by sentence, then by word
    cleaned_words = [w for w in nltk_words if not w in stops]  # Remove stop words
    lemmatized_words = [wnl.lemmatize(w) for w in cleaned_words]  # stem words
    # stemmed_words = [stemmer.stem(w) for w in cleaned_words] #stem words

    return (" ".join(lemmatized_words))


def delete_dir(folder_name):
    folder = folder_name
    for the_file in os.listdir(folder):
        print('delete')
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                print(file_path)
                os.unlink(file_path)
        except Exception as e:
            print(e)

"""
================================================================================
 ### Main function
================================================================================        
"""

def main(input):
    pool = mp.Pool()
    chunks = split(input)
    print(chunks)
    result = pool.map(run, chunks)
    pool.close()
    pool.join()
    combined = pd.concat(result)
    combined = combined.reset_index()
    combined.to_csv('resources/preprocessed_results.csv', encoding='utf-8', index=False)  # save to file
    print(combined)
    delete_dir('resources/chunks') #delete files of chunks



def run(input):
    print('preprocessing run')

    review_data = pd.read_csv(input)  # open file
    review_data = review_data.replace('\n', ' ')

    review_data['text'] = review_data['text'].apply(textCleaning)  # clean text
    review_data['bag_of_words'] = review_data['text'].apply(textPrepocessing)  # create bag of word
    review_data['yelp_rating'] = review_data['yelp_rating'].fillna(0)  # fill empty fields with 0
    return review_data


"""
================================================================================
### Split files for parallel processing
================================================================================        
"""

def split(input):
    size = sum(1 for line in open(input))
    num_cpus = mp.cpu_count()
    total = size / num_cpus
    for i, chunk in enumerate(pd.read_csv(input, chunksize=total + 1)):
        chunk.to_csv('resources/chunks/part{}.csv'.format(i), index=False)
    files = os.listdir('resources/chunks')
    files_path = list()
    for x in files:
        files_path.append('resources/chunks/' + x)
    return files_path



