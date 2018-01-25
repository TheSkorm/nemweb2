from elasticsearch import Elasticsearch
import nem
import urllib
import arrow
from dateutil import tz

es = Elasticsearch()
mapping='''
{
  "mappings": {
    "scada": {
      "numeric_detection": true
    }
  }
}
'''
es.indices.create(index='scada', ignore=400, body=mapping)


for data in nem.historical.SCADA:
    try:
        for dataset, rows in data.clean_data.items():
            for row in rows:
                ts = arrow.get(row['SETTLEMENTDATE'], 'YYYY/MM/DD HH:mm:ss', tzinfo=tz.gettz('Australia/Brisbane')).naive
                row["timestamp"] = ts
                res = es.index(index="scada", doc_type='scada', body=row)
    except urllib.error.HTTPError:
        print("Could not download " + data.url)
    