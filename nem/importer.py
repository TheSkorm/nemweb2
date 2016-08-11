import csv
from io import BytesIO, TextIOWrapper
from urllib.request import urlopen
from zipfile import ZipFile

from bs4 import BeautifulSoup


class importer():
    def __init__(self, url="http://www.nemweb.com.au/Reports/CURRENT/"):
        self.p5 = files(url + "P5_Reports/")
        self.DispatchIS = files(url + "DispatchIS_Reports/")
        self.Notices = files(url + "Market_Notice/")
        self.SCADA = files(url + "Dispatch_SCADA/")
        self.CO2 = files(url + "CDEII/")


class files(list):
    def __init__(self, baseUrl):
            indexPage = BeautifulSoup(urlopen(baseUrl).read(), "html.parser")
            for link in indexPage.find_all('a')[1:]:
                url = link.get('href').split("/")[-1]
                self.append(document(baseUrl + url))


class document():
    def __init__(self, url):
        self.url = url
        self._cached = None

    @property
    def data(self):
        if not self._cached:
            if self.url[-4:].lower() != ".csv":
                self._cached = self._extract(self.url)
            else:
                self._cached = urlopen(self.url).read()
            self._cached = self._cached.decode('utf-8').split("\n")
        return self._cached

    def _extract(self, url):
        file = ZipFile(BytesIO(urlopen(url).read()))
        data = file.read(file.namelist()[0])
        return data

    def filter(self, dataSet):
        csvfile = csv.reader(self.data)
        headers = []
        data = []
        for row in csvfile:
            try:
                if (row[0] == "I" and row[2] == dataSet):
                    headers = row
                elif row[2] == dataSet:
                    data.append({headers[ind]: x for ind, x in enumerate(row)})
            except(IndexError):
                pass
        return data
