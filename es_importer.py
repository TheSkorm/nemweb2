from elasticsearch import Elasticsearch
import nem
import urllib
import arrow
from dateutil import tz

file_list = ["http://www.nemweb.com.au/Reports/Current/CDEII/CO2EII_AVAILABLE_GENERATORS_2017_50_20171222134123.CSV", "http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2011/MMSDM_2011_03/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_BIDPEROFFER_201103010000.zip"]




# for data in nem.historical.SCADA:
#     try:
#         for dataset, rows in data.clean_data.items():
#             for row in rows:
#                 ts = arrow.get(row['SETTLEMENTDATE'], 'YYYY/MM/DD HH:mm:ss', tzinfo=tz.gettz('Australia/Brisbane')).naive
#                 row["timestamp"] = ts
#                 res = es.index(index="scada", doc_type='scada', body=row)
#     except urllib.error.HTTPError:
#         print("Could not download " + data.url)
    
for url in file_list:
    print(url)
    data = nem.document(url)
    try:
        for dataset, rows in data.clean_data.items():
            indice_name= dataset +"-t-" + data.dateTime.strftime("%Y%m%d")
            es = Elasticsearch()
            mapping={
                "mappings": {
                    dataset: {
                    "numeric_detection": True
                    }
                }
            }
            es.indices.create(index=indice_name, ignore=400, body=mapping)
            for row in rows:
                print(row)
                row["file_timestamp"] = data.dateTime.isoformat() #enrich the data with information from the file metadata
                row["file_name"] = data.url
                res = es.index(index=indice_name, doc_type=dataset, body=row)
    except urllib.error.HTTPError:
        print("Could not download " + data.url)
    