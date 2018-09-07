# -*- coding: utf-8 -*-
import requests
import csv
import json
url1 = 'https://api.foursquare.com/v2/venues/search'
url2 = 'https://api.foursquare.com/v2/venues/'

output_saved = "tempstore_foursquare.txt"

def search_id(lat, long, name, poiid, csv):
    params = dict(
        client_id='',
        client_secret='',
        v='20180323',
        ll=lat + ',' + long,
        radius=200,
        query=name,
        intent='match',
        limit=1
        )
    resp = requests.get(url=url1, params=params)
    data = json.loads(resp.text)
    print data
    if len(data['response']['venues']) > 0:
        id = data['response']['venues'][0]['id']
        with open(csv, "a") as file:
            file.write('\n' + poiid + ',' + id)
        return id
    else:
        return 0



def search_place(id):
    params = dict(
        client_id='',
        client_secret='',
        v='20180323',
        limit=1
        )
    resp = requests.get(url=url2+id, params=params)
    data = json.loads(resp.text)
    return(data)

def search_tips(id):
    params = dict(
        client_id='',
        client_secret='',
        v='20180323',
        limit=1
        )
    resp = requests.get(url=url2+id+'/tips', params=params)
    data = json.loads(resp.text)
    print 'Tips:' + data
    return data['response']['tips']['items'][0]['text']

def get_attribute(result,attribute):
    return result.get(attribute) or 'none'

def query_fs(lat, long, name, poiid, output_csv):
    id = search_id(lat, long, name)
    if id != 0:
        tips = ""
        result = search_place(id)
        print result
        if result['response']:
            print result['response']
            #result = result['response']['venue']
            if 'stats' in result['response']['venue']:
                checkinsCount = result['response']['venue']['stats']['checkinsCount']
                print checkinsCount
            else:
                checkinsCount = 0
                print('test')
            try:
              categories = result['response']['venue']['categories'][0]['pluralName']
              print categories
            except:
                categories = 'none'
                pass
            try:
                priceRange = (result['response']['venue']['price']['message'])
            except:
                priceRange = 'none'
                pass
            try:
                rating = (result['response']['venue']['rating'])
            except:
                rating = 'none'
                pass
            try:
                tipsCount = (result['response']['venue']['tips']['count'])
                if tipsCount > 0:
                    tips = search_tips(id)
            except:
                tips = 'none'
                pass
            try:
                likesCount = (result['response']['venue']['likes']['count'])
            except:
                likesCount = 0
                pass
            try:
                phrases = (result['response']['venue']['phrases'])
            except:
                phrases = 'none'
                pass

            text = poiid + "," + str(checkinsCount) + "," + categories.encode('utf-8').strip().replace(',', '') + "," + \
               priceRange + "," + str(rating) + "," + str(tipsCount) + "," + "," + str(likesCount) + "," +  \
               phrases.encode('utf-8').strip().replace(',', '') + "," + tips
            print text
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


def run(input_file, output_csv):

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
        if(len(record) == 5):
            poiid = record[0]
            name = record[1]
            latitude = record[3]
            longitude = record[4]
            print("Querying Foursquare: name: "+str(name)+" lat: "+str(latitude)+" long: "+longitude)
            with open(output_saved, mode="w") as file:
                file.write(poiid)
            print("ready")
            search_id(latitude, longitude, name, poiid, output_csv)
            with open('done_fs.csv', mode="a") as file:
                file.write(poiid + '\n')

        else:
            print("Illegal record: " + str(record))


run('resources/data.csv', 'foursquare_id.csv')