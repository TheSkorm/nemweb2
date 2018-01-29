from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
import nem
import urllib
from dateutil import tz
import time
from nem.importer import files
from queue import Queue
import threading
import uuid

file_list = ["http://www.nemweb.com.au/Reports/Current/CDEII/CO2EII_AVAILABLE_GENERATORS_2017_50_20171222134123.CSV", "http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2011/MMSDM_2011_03/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_BIDPEROFFER_201103010000.zip"]

# PUBLIC_DVD_GENUNITS_201711010000.zip

file_list = files(baseUrl="http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/", historical="PUBLIC_DVD_DISPATCHREGIONSUM")
    
# Set up some global variables
num_fetch_threads = 4
enclosure_queue = Queue()


def download(url):
        print(url)
        data = nem.document(url)
        try:
            for dataset, rows in data.clean_data.items():
                indice_name= (dataset +"-t-" + data.dateTime.strftime("%Y%m%d")).lower()
                es = Elasticsearch()
                mapping={
                    "mappings": {
                        dataset: {
                        "numeric_detection": True
                        }
                    }
                }
                es.indices.create(index=indice_name, ignore=400, body=mapping)
                actions=[]
                for row in rows:
                    row["file_timestamp"] = data.dateTime.isoformat() #enrich the data with information from the file metadata
                    row["file_name"] = data.url
                    pindex = {
                        "_index": indice_name,
                        "_type": dataset,
                        "_id": str(uuid.uuid4()),
                        "doc" : row
                    }
                    actions.append(pindex) #this should really be an interator so we don't need to put everything in memory but for now we'll throw money at the problem
                for r in parallel_bulk(es, actions, 8, raise_on_error=True, raise_on_exception=True):
                    pass
        except urllib.error.HTTPError:
            print("Could not download " + data.url)



def process(q):
    while True:
        url = q.get()
        download(url)
        q.task_done()

# Set up some threads to fetch the enclosures
for i in range(num_fetch_threads):
    worker = threading.Thread(
        target=process,
        args=(enclosure_queue,),
        name='worker-{}'.format(i),
    )
    worker.setDaemon(True)
    worker.start()

for url in file_list:
    url = str(url)
    enclosure_queue.put(url)

enclosure_queue.join()

# for filen in file_list:
#     download(str(filen))