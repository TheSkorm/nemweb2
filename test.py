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
downloaded = dynamodb.Table('downloaded')

nemImporter = nem.importer()

for file in nemImporter.p5:
    fileCheck = downloaded.get_item(Key={'url': str(file)})
    if 'Item' in fileCheck:
        pass
    else:
        print("Downloading " + str(file))
        downloaded.put_item(Item={'url': str(file)})
        rows = file.filter("REGIONSOLUTION")
        for payload in rows:
            payload['index'] = payload['INTERVAL_DATETIME'] + payload['REGIONID']
            response = p5Table.put_item(Item=payload) #this should be a batch put item