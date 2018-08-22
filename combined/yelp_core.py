# -*- coding: utf-8 -*-

from graphqlclient import GraphQLClient
import os
import csv
import json
import sys
output_saved = "tempstore_neu.txt"


def query_yelp(name, latitude, longitude, token, output_dir, poiid, output_csv, category):
    client = GraphQLClient('https://api.yelp.com/v3/graphql')
    client.inject_token(token)
    print("ready2")
    result = client.execute("\
        {\
            search(term: \"" + name + "\", latitude: " + latitude + ", longitude: " + longitude + ", radius: 200) {\
              business {\
                name\
                id\
                is_closed\
                price\
                rating\
                review_count\
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

    handle_yelp_results(result, output_dir, poiid, output_csv, category)


def handle_yelp_results(result, output_dir,poiid, output_csv, category):
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
                          business['name'].replace("/", "").replace(" ", "").replace("'", "")\
                          + "_" + \
                          review['id'] + \
                          ".txt "
            write_file(output_file, poiid + ',' + review['text'].encode('utf-8').strip())
            text = poiid + "," + business['id'] + "," + business['name'].encode('utf-8').strip().replace(',', '') + "," + review['id'] + ",'" + review['text'].encode('utf-8').strip().replace(',', '').replace('"', '').replace('\n', ' ') + "',"
            for category in business['categories']:
                text += category['title'].encode('utf-8').strip() + " "
            text += "," + str(business['rating']) + "," + category
            text += "," + str(business['is_closed'])
            text += "," + str(business['price'])
            text += "," + str(business['review_count']) + ","
            with open(output_csv, "a") as file:
                file.write('\n' + text)


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
    print("Number of records loaded: " + str(len(records)))
    if(count==len(records)):
        count = 0
    try:
        if not os.path.exists(os.path.dirname(output_dir)):
            os.makedirs(os.path.dirname(output_dir))
    except OSError as err:
        print(err)
    for record in records[count:]:
        if(len(record) == 5):
            poiid = record[0]
            name = record[1]
            category = record[2]
            latitude = record[3]
            longitude = record[4]
            print("Querying YELP: name: "+str(name)+" lat: "+str(latitude)+" long: "+longitude)
            with open(output_saved, mode="w") as file:
                file.write(poiid)
            print("ready")
            query_yelp(name, latitude, longitude, token, output_dir, poiid, output_csv, category)
            with open('done.csv', mode="a") as file:
                file.write(poiid + '\n')

        else:
            print("Illegal record: " + str(record))

