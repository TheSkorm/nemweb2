import csv
import re
import os
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

from bs4 import BeautifulSoup

# TODO DANGEROUS
os.environ['TZ'] = 'Australia/Brisbane' #nem time is always in Sydney time
print("This library has updated your timezone to Brisbane - I'm sorry")

OLDEST_YEAR = 2009
OLDEST_MONTH = 7
class importer():
    """Impoter class allows downloading data from AEMO NEM site

    :param url:
        URL to the NEM server
    :type url: ``string``

    :param historical:
        Access to historical data using the MMSDM data
    :type historical: ``boolean``

    :returns: Returns an array of documents
    :rtype: ``[document]``
    """
    def __init__(self, url="http://www.nemweb.com.au/Reports/CURRENT/", historical=False):
        if historical==False:
            self.p5 = files(url + "P5_Reports/", historical)
            """NEM p5 data documents
            
            :rtype: [document]
            """
            self.DispatchIS = files(url + "DispatchIS_Reports/", historical)
            """NEM DispatchIS data documents
            
            :rtype: [document]
            """
            self.Notices = files(url + "Market_Notice/", historical)
            """NEM Notices data documents
            
            :rtype: [document]
            """
            self.SCADA = files(url + "Dispatch_SCADA/", historical)
            """NEM SCADA data documents
            
            :rtype: [document]
            """
            self.CO2 = files(url + "CDEII/", historical)
            """NEM CO2 data documents
            
            :rtype: [document]
            """
        else:
            self.p5 = files(url, "PUBLIC_DVD_P5MIN_REGIONSOLUTION")
            self.DispatchIS = files(url, "PUBLIC_DVD_DISPATCHREGIONSUM")
            # self.Notices = files(url + "Market_Notice/", historical) #TODO
            self.SCADA = files(url, "PUBLIC_DVD_DISPATCH_UNIT_SCADA")
            self.CO2 = files(url, "PUBLIC_DVD_BILLING_CO2E_PUBLICATION") #this is wrong

class files(list):
    """Lists all the documents avaliable for parsing by this module

    :param baseUrl:
        URL to list files for
    :type baseUrl: ``string``

    :param historical:
        Enables generating a list of files for every year, requires being set to the filename requested (eg PUBLIC_DVD_P5MIN_REGIONSOLUTION)
    :type historical: ``string``

    :returns: Returns an array of documents
    :rtype: ``[document]``
    """
    def __init__(self, baseUrl, historical):
        if historical == False:
            indexPage = BeautifulSoup(urlopen(baseUrl).read(), "html.parser")
            for link in indexPage.find_all('a')[1:]:
                url = link.get('href').split("/")[-1]
                if len(url) > 2 and url[-1] != "/":
                    self.append(document(baseUrl + url))
        else:
            for year in range(OLDEST_YEAR,datetime.now().year+1):
                for month in range(1,13):
                    if year == OLDEST_YEAR and month < OLDEST_MONTH: #skip oldest months
                        continue
                    if year == datetime.now().year and  month >= datetime.now().month: # skip months that haven't been stored yet
                        continue


                    url = '{}{}/MMSDM_{}_{:02d}/MMSDM_Historical_Data_SQLLoader/DATA/{}_{}{:02d}010000.zip'.format(baseUrl, year, year, month, historical, year, month)
                    self.append(document(url))
                            


class document():
    """Lists all the documents avaliable for parsing by this module

    :param url:
        URL to parse
    :type url: ``string``
    """
    def __init__(self, url):
        self.url = url
        self._cached = None
        nemDateRegex = re.compile('(2\d{11}(?:\d{2})?)[\._]')
        try:
            strDate = nemDateRegex.findall(url)[0]
            try:
                dateObject = datetime.strptime(strDate, "%Y%m%d%H%M")
            except ValueError:
                try:
                    dateObject = datetime.strptime(strDate, "%Y%m%d")
                except ValueError:
                    dateObject = datetime.strptime(strDate, "%Y%m%d%H%M%S")
            self.dateTime = dateObject
            """Provides a datetime object for the file based on the timestamp in the filename

            :rtype: datetime
            """
        except(IndexError):
            pass


    def __str__(self):
        return self.url

    def __repr__(self):
        return self.url

    @property
    def data(self):
        """Provides the raw data from the file

        :rtype: list
        :returns: Array of raw lines of data
        """
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
    @property
    def datasets(self):
        """Provides lists of datasets in the file

        :rtype: list
        :returns: Array of datasets avaliable
        """
        csvfile = csv.reader(self.data)
        data = []
        for row in csvfile:
            try:
                if (row[0] == "I"):
                    data.append(row[2])
            except(IndexError):
                pass
        return data
    @property
    def clean_data(self):
        """Provides a dictonary of all the datasets

        :rtype: object
        :returns: {"dataset1":[{"header1":"value"}]}
        """
        csvfile = csv.reader(self.data)
        headers = []
        data = {}
        dataset = ""
        table = ""
        for row in csvfile:
            try:
                if (row[0] == "I" ):
                    headers = row
                    dataset = (row[1] + "-" + row[2]).lower() #prepend with the setp.world variable.
                    data[dataset] = []
                    table = row[2]
                elif row[2] == table:
                    rowCleaned = {headers[ind]: self.string_to_float(x) for ind, x in enumerate(row) if x != ''}
                    data[dataset].append(rowCleaned)

            except(IndexError):
                pass
        return data
    def string_to_float(self,x):
        try:
            x = int(x)
        except ValueError:
            try:
                x = float(x)
            except ValueError:
                pass
            
        return x    
    def filter(self, dataSet):
        """Filters out data to a specific dataset and return that set

        :rtype: [{"header1":"data1"}]
        """
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
