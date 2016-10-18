import configparser
import os

import nem
import boto3


config = configparser.ConfigParser()
config.read('config.ini')

os.environ["AWS_ACCESS_KEY_ID"] = config["aws"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["aws"]["AWS_SECRET_ACCESS_KEY"]

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')

p5Table = dynamodb.Table('p5-regionalsolution')
downloaded = dynamodb.Table('downloaded') #change this to be "the last file downloaded" so it doesn't need to check each file is downloaded or not

nemImporter = nem.importer()

try:
    latestData = downloaded.get_item(Key={'url': 'p5-regionalsolution'})['value']['date']
except(KeyError):
    latestData = '0'

for nemFile in nemImporter.p5:
    if int(nemFile.dateTime) > int(latestData):
        print("Downloading " + str(nemFile))
        downloaded.put_item(Item={'url': str(nemFile)})
        rows = nemFile.filter("REGIONSOLUTION")
        for payload in rows:
            payload['index'] = payload['INTERVAL_DATETIME'] + payload['REGIONID']
            response = p5Table.put_item(Item=payload) #this should be a batch put item
        downloaded.put_item(Item={'url':"p5-regionalsolution", 'date':nemFile.dateTime})
    else:
        print("Skipping " + str(nemFile))
