# -*- coding: utf-8 -*-

from graphqlclient import GraphQLClient
import os
import csv
import json

def query_yelp(name, latitude, longitude, token, output_dir):
    client = GraphQLClient('https://api.yelp.com/v3/graphql')
    client.inject_token(token)

    result = client.execute("\
        {\
            search(term: \"" + name + "\", latitude: " + latitude + ", longitude: " + longitude + ", radius: 200) {\
              business {\
                name\
                id\
                reviews {\
                  id\
                  text\
                }\
              }\
            }\
        }\
        ")

    handle_yelp_results(result, output_dir)

def handle_yelp_results(result, output_dir):
    json_data = json.loads(result)

    if(json_data['data']['search']['business'] == None):
        print("No result")
        return

    business_list = json_data['data']['search']['business']

    if len(business_list) > 0:
        business = business_list[0]
        print("Found business " + business['name'] + " with id " + business['id'])
        for review in business['reviews']:
            print("Found review " + review['id'])
            output_file = output_dir + "/" + \
                          business['id'] + "_" + \
                          business['name'].replace("/", " ") + "_" + \
                          review['id'] + \
                          ".txt "
            write_file(output_file, review['text'])

def read_csv(input_file):
    records = []
    counter = 0

    with open(input_file) as csvDataFile:
        csv_reader = csv.reader(csvDataFile, delimiter=";")
        for row in csv_reader:
            records.append(row)
            counter += 1
            # print(row)

    return records

def write_file(output_file, content):
    with open(output_file, mode="w") as file:
        file.write(content)

def run(input_file, output_dir, token):
    records = read_csv(input_file)
    print("Number of records loaded: " + str(len(records)))
    os.makedirs(output_dir, exist_ok=True)
    for record in records:
        if(len(record) == 4):
            poiid = record[0]
            name = record[1]
            latitude = record[2]
            longitude = record[3]
            print("Querying YELP: POI-ID:"+str(poiid)+" name: "+str(name)+" lat: "+str(latitude)+" long: "+longitude)
            query_yelp(name, latitude, longitude, token, output_dir)
        else:
            print("Illegal record: " + str(record))


run(
    #input_file="/home/ubuntu16/Documents/slipo/tomtom_pois_austria_v0.3_mcdonalds_unique_no_duplicates.csv",
    input_file="/home/ubuntu16/Documents/slipo/tomtom_pois_austria_v0.3_unique2_no_duplicates_todo.csv",
    #input_file="/home/ubuntu16/Documents/slipo/sample_tomtom_pois_austria_v0.3_unique.csv",
    output_dir="/home/ubuntu16/Documents/slipo/yelp_reviews_4",
    token='Bearer CCvhugbVKkeuraDE3o1czjko33PH3mB3GmOK8g8zAtBB1wc0iZXIV1TkiDLeHG5Ju_LO0APvgMvgFcQYxU96n0CbVzThdO6UculNyBjW3GPue_ql_lKwepvyQcIaW3Yx',
)
