# encoding=utf8
import subprocess
import os
from rdflib import Graph
from rdflib import URIRef, Literal
import re
import pandas as pd
import random
import string
import shutil
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def run_sentiments(input, output_dir):
    g = Graph()

    #create new random directory name for output files of nlp, if directory already exists program will fail
    if os.path.exists(os.path.dirname(output_dir)):
        output_dir = output_dir + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))
        print(output_dir)

    g.parse("resources/slipo_keywords.nt", format="nt")

    hasSentiment = URIRef("http://slipo.eu/hasSentiment")
    hasPerson = URIRef("http://slipo.eu/hasPerson")
    hasOrganization = URIRef("http://slipo.eu/hasOrganization")
    hasLocation = URIRef("http://slipo.eu/hasLocation")

    subprocess.call('spark-submit --class PreprocSpark --packages databricks:spark-corenlp:0.2.0-s_2.11 --jars ' #call new subprocess to execute nlp program
                   'resources/stanford-english-corenlp-2016-10-31-models.jar resources/sparknlp_2.11-0.1.jar '
                   '' + input + ' ' + output_dir, shell=True)

    list = os.listdir(output_dir)
    for item in list:
        sentfile = open(output_dir + "/" + item, 'r')
        text = sentfile.read()

        if(item != '_SUCCESS' and item[0]!= "."):
            poiid = text.split(',')[0]
            poiid = URIRef(poiid.replace(' ', '').replace('|0', '').replace('|O',''))
            sentiment = re.findall('\[.*?\]',text) #get sentiment between the brackets
            total = 0
            for sent in sentiment:
                try:
                    total += int(sent[1])
                except:
                    pass
            total = total/len(sentiment)
            #total score per review -- average over reviews later on
            with open('resources/review_score.csv', mode="a") as file:
                file.write(('\n' + str(poiid) + "," + str(total)))

            # add NER keywords
            pureText = re.split('\W+', text)
            i = 0
            while i < len(pureText):
                word = pureText[i]
                if word == "PERSON":
                    g.add((poiid, hasPerson, Literal(pureText[i-1])))
                if word == "ORGANIZATION":
                    g.add((poiid, hasOrganization, Literal(pureText[i-1])))
                if word == "LOCATION":
                    g.add((poiid, hasLocation, Literal(pureText[i-1])))

                i += 1

    #add average review sentiment score to nt file

    sent_data = pd.read_csv('resources/review_score.csv')
    sent_data = sent_data.groupby(['poiid'])['score'].mean()
    sent_data = sent_data.reset_index()
    sent_data['poiid'] = sent_data['poiid'].astype(basestring)
    sent_data['score'] = sent_data['score'].astype(basestring)
    m = 0
    while m < len(sent_data):
        g.add((URIRef(sent_data['poiid'][m].replace(' ', '')), hasSentiment, Literal("{0:.1f}".format(sent_data['score'][m]))))
        m += 1

    g.serialize(destination="resources/slipo_keywords.nt", format='nt')
    shutil.rmtree(output_dir)  #new directory is deleted
    #os.system("rm -rf " + output_dir)


def run_keywords(output_dir):
    g = Graph()

    hasKeyword = URIRef("http://slipo.eu/hasKeyword")
    hasYelpCategory = URIRef("http://slipo.eu/hasYelpCategory")
    hasTomtomCategory = URIRef("http://slipo.eu/hasTomtomCategory")
    hasRating = URIRef("http://slipo.eu/hasRating")
    isClosed = URIRef("http://slipo.eu/isClosed")
    reviewCount = URIRef("http://slipo.eu/hasNoReviews")
    hasPriceLevel = URIRef("http://slipo.eu/hasPriceLevel")

    data = pd.read_csv(output_dir)
    n = 0
    while n<len(data):
        poiid = URIRef(data['poiid'][n].replace(' ', '')) #getting rid of whitespace to avoid erros

        keywords = (data['keywords'][n])
        keyword_phrase = re.findall('\(.*?\)', keywords)  #get single keyword phrases

        for phrase in keyword_phrase:
            phrase = phrase.split(',') #getting rid of numbers
            text = phrase[0].replace('(','')  #getting rid of opening parenthesis in the beginning
            g.add((poiid, hasKeyword, Literal(text)))

        categories = data['yelp_category'][n]
        category = categories.split(";")
        for text in category:
            text = text.replace('[', '').replace(']', '').replace("'", '').strip()
            if text != "" and text != "0":
                g.add((poiid, hasYelpCategory, Literal(text)))

        rating = data['yelp_rating'][n]
        if rating != 0.0:
            g.add((poiid, hasRating, Literal(rating)))

        tomtomCategories = str(data['tomtom_category'][n])
        tomtomCategory = tomtomCategories.split(";")
        for text in tomtomCategory:
            texttomtom = text.replace('[', '').replace(']', '').replace("'", '').strip()
            if texttomtom != "" and texttomtom != "0":
                g.add((poiid, hasTomtomCategory, Literal(texttomtom)))

        is_closed = str(data['is_closed'][n])
        if is_closed != 'none' and is_closed != 'None':
            g.add((poiid, isClosed, Literal(is_closed)))

        review_count = (data['review_count'][n])
        if is_closed != 'none':
            g.add((poiid, reviewCount, Literal(int(review_count))))

        price_level = data['price'][n]
        if price_level != 'none' and price_level != 'None':
            if price_level == '€':
                g.add((poiid, hasPriceLevel, Literal('low')))
            if price_level == '€€':
                g.add((poiid, hasPriceLevel, Literal('medium-low')))
            if price_level == '€€€':
                g.add((poiid, hasPriceLevel, Literal('medium-high')))
            if price_level == '€€€€':
                g.add((poiid, hasPriceLevel, Literal('high')))
        n += 1

    g.serialize(destination='resources/slipo_keywords.nt', format='nt')

#generated nt file does not show unicode symbols properly, to pretty print them use following function
def to_unicode():
    with open('resources/slipo_keywords.nt') as f:
        for line in f:
            print(line)
            new_line = (line.decode('unicode_escape'))
            print(new_line)
            file_new = open('resources/slipo_keywords.nt', 'a')
            file_new.write(new_line)

