# -*- coding: utf-8 -*-

from graphqlclient import GraphQLClient
import os
import csv
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


output_saved = "resources/tempstore.txt"

def query_yelp(name, latitude, longitude, token, output_dir, poiid, output_csv):
    client = GraphQLClient('https://api.yelp.com/v3/graphql')
    client.inject_token(token)
    print("ready2")
    result = client.execute("\
        {\
            search(term: \"" + name + "\", latitude: " + latitude + ", longitude: " + longitude + ", radius: 200) {\
              business {\
                name\
                id\
                rating\
                categories {\
                     title\
                 }\
                reviews {\
                  id\
                  text\
                }\
              }\
            }\
        }\
")

    handle_yelp_results(result, output_dir, poiid, output_csv)

def handle_yelp_results(result, output_dir,poiid, output_csv):
    json_data = json.loads(result)

    if(json_data['data']['search']['business'] == None):
     print("No result")
     return

    business_list = json_data['data']['search']['business']

    if len(business_list) > 0:
        business = business_list[0]
        print("Found business " + business['name'].encode('utf-8').strip() + " with id " + business['id'].encode('utf-8'))
        for category in business['categories']:
            print(category['title'])
        for review in business['reviews']:
            print("Found review " + review['id'].encode('utf-8').strip())
            output_file = output_dir + "/" + \
                          business['id'].encode('utf-8').strip() + "_" + \
                          business['name'].replace("/", "").replace(" ", "").replace("'", "").encode('utf-8').strip() + "_" + \
                          review['id'] + \
                          ".txt "
            write_file(output_file, poiid + ',' + review['text'].encode('utf-8').strip())
            text = poiid + business['id'] + business['name'].encode('utf-8').strip().replace(',', '') + review['id'] + ",'" + review['text'].encode('utf-8').strip().replace(',', '').replace('\n', ' ') + "',"
            for category in business['categories']:
                text += category['title'].encode('utf-8').strip() + " "
            text += "," + str(business['rating'])
            with open(output_csv, "a") as file:
                file.write('\n' + text)
                #file.write('\n')


def read_csv(input_file):
    records = []
    counter = 0

    with open(input_file) as csvDataFile:
        csv_reader = csv.reader(csvDataFile, delimiter=";")
        for row in csv_reader:
            records.append(row)
            counter += 1

    return records

def write_file(output_file, content):
    with open(output_file, mode="w") as file:
        file.write(content.encode('utf-8').strip())

def run(input_file, output_dir,output_csv, token):
    with open(output_saved, mode="r") as file:
        lastread = file.read()
    records = read_csv(input_file)
    count = 0
    for record in records:
        if(lastread==record[0]):
            break
        else:
            count += 1
    print(count)
    #print(records[count])
    print("Number of records loaded: " + str(len(records)))
    if(count==len(records)):
        count = 0
    try:
        if not os.path.exists(os.path.dirname(output_dir)):
            os.makedirs(os.path.dirname(output_dir))
    except OSError as err:
        print(err)
    for record in records[count:]:
        if(len(record) == 4):
            poiid = record[0]
            name = record[1]
            latitude = record[2]
            longitude = record[3]
            print("Querying YELP: name: "+str(name)+" lat: "+str(latitude)+" long: "+longitude)
            with open(output_saved, mode="w") as file:
                file.write(poiid)
            print("ready")
            query_yelp(name, latitude, longitude, token, output_dir, poiid, output_csv)

        else:
            print("Illegal record: " + str(record))


#run(
#    #input_file="/home/ubuntu16/Documents/slipo/tomtom_pois_austria_v0.3_mcdonalds_unique_no_duplicates.csv",
#    input_file="/Users/Beate/Desktop/slipo/tomtom_pois_austria_v0.3_unique_no_duplicates.csv",
#    #input_file="/home/ubuntu16/Documents/slipo/sample_tomtom_pois_austria_v0.3_unique.csv",
#    output_dir="/Users/Beate/Desktop/slipo/output_yelpTESTdd",
#    token='Bearer CCvhugbVKkeuraDE3o1czjko33PH3mB3GmOK8g8zAtBB1wc0iZXIV1TkiDLeHG5Ju_LO0APvgMvgFcQYxU96n0CbVzThdO6UculNyBjW3GPue_ql_lKwepvyQcIaW3Yx',
#)
