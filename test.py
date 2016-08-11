import configparser
import os

import nem
import boto3


config = configparser.ConfigParser()
config.read('config.ini')

os.environ["AWS_ACCESS_KEY_ID"] = config["aws"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["aws"]["AWS_SECRET_ACCESS_KEY"]

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')

table = dynamodb.Table('p5-regionalsolution')

nemImporter = nem.importer()
payload = nemImporter.p5[0].filter("REGIONSOLUTION")[0]
payload['index'] = payload['INTERVAL_DATETIME'] + payload['REGIONID']

response = table.put_item(Item=payload)
print(response)

response = table.scan()
print(response)