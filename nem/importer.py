import csv
import re
import os
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import zipfile
from bs4 import BeautifulSoup

os.environ['TZ'] = 'Australia/Sydney'  # nem time is always in Sydney time


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
        nemDateRegex = re.compile('_(2\d{7,11})(?:\d{0,2})[\._]')
        try:
            strDate = nemDateRegex.findall(url)[0]
            try:
                dateObject = datetime.strptime(strDate, "%Y%m%d%H%M")
            except ValueError:
                dateObject = datetime.strptime(strDate, "%Y%m%d")
            self.dateTime = dateObject
        except(IndexError):
            pass

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.url

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
        try:
            file = ZipFile(BytesIO(urlopen(url).read()))
            data = file.read(file.namelist()[0])
            return data
        except zipfile.BadZipFile:
            print("Bad data in " + url)
            pass

    def filter(self, dataSet):
        try:
            csvfile = csv.reader(self.data)
            headers = []
            data = []
            for row in csvfile:
                try:
                    if (row[0] == "I" and row[2] == dataSet):
                        headers = row
                    elif row[2] == dataSet:
                        rowCleaned = {headers[ind]: x for ind, x in enumerate(row) if x != ''}
                        data.append(rowCleaned)
                except(IndexError):
                    pass
            return data
        except AttributeError:
            print("Couldn't read file " + self.url)
            pass