# encoding=utf8
# !/usr/bin/env python

import tweepy
import csv

output_saved = "resources/tempstore_twitter.txt"


def search_twitter(poiid, name, lat, long):
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    csvfile = 'resources/twitter_out.csv'

    api = tweepy.API(auth)
    results = api.search(q=name, geocode=lat + "," + long + ",0.2km", count=100)
    for result in results:
        print (result.text.encode('utf-8'))
        with open(csvfile, "a") as file:
            file.write('\n' + poiid + ',' + result.text.encode('utf-8').strip().replace(',', '').replace('"', '').replace('\n', ' '))


def run(input_file):

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
    if count == len(records):
        count = 0

    for record in records[count:]:
        if(len(record) == 4):
            poiid = record[0]
            name = record[1]
            latitude = record[2]
            longitude = record[3]
            print("Querying Twitter: name: "+str(name)+" lat: "+str(latitude)+" long: "+longitude)
            with open(output_saved, mode="w") as file:
                file.write(poiid)
            print("ready")
            search_twitter(poiid, name, latitude, longitude)
            with open('done_twitter.csv', mode="a") as file:
                file.write(poiid + '\n')

        else:
            print("Illegal record: " + str(record))


def read_csv(input_file):
    records = []
    counter = 0

    with open(input_file) as csvDataFile:
        csv_reader = csv.reader(csvDataFile, delimiter=";")
        for row in csv_reader:
            records.append(row)
            counter += 1

    return records

run('tomtom_pois_austria_v0.3_unique.csv')