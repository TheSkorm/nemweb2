from elasticsearch import Elasticsearch
import nem
import urllib
from dateutil import tz
import time
from nem.importer import files

file_list = ["http://www.nemweb.com.au/Reports/Current/CDEII/CO2EII_AVAILABLE_GENERATORS_2017_50_20171222134123.CSV", "http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2011/MMSDM_2011_03/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_BIDPEROFFER_201103010000.zip"]

# PUBLIC_DVD_GENUNITS_201711010000.zip

file_list = files(baseUrl="http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/", historical="PUBLIC_DVD_DISPATCHPRICE")
    
for url in file_list:
    url = str(url)
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