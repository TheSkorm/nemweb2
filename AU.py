# test parser for http://www.electricitymap.org/

# The arrow library is used to handle datetimes
import arrow
# The request library is used to fetch content through HTTP
import requests
import json
import pandas as pd
import math
import nem
from pyexcel_xls import get_data
from io import BytesIO


AMEO_CATEGORY_DICTIONARY = {
    'Bagasse': 'biomass',
    'Black Coal': 'coal',
    'Brown Coal': 'coal',
    'coal': 'coal',
    'Coal Seam Methane': 'gas',
    'Diesel': 'oil',
    'gas': 'gas',
    'Macadamia Nut Shells': 'biomass',
    'hydro': 'hydro',
    'Hydro': 'hydro',
    'Kerosene': 'oil',
    'Landfill / Biogas': 'biomass',
    'Landfill / Biogass': 'biomass',
    'Landfill Gas': 'biomass',
    'Landfill Methane / Landfill Gas': 'biomass',
    'Landfill, Biogas': 'biomass',
    'Macadamia Nut Shells': 'biomass',
    'Natural Gas': 'gas',
    'Natural Gas / Diesel': 'gas',
    'Natural Gas / Fuel Oil': 'gas',
    'oil': 'oil',
    'Sewerage/Waste Water': 'biomass',
    'Solar': 'solar',
    'Solar PV': 'solar',
    'Waste Coal Mine Gas': 'gas',
    'Waste Water / Sewerage': 'biomass',
    'Water': 'hydro',
    'Wind': 'wind'
}


PRICE_MAPPING_DICTIONARY = {
    'AUS-NSW': 'NSW1',
    'AUS-QLD': 'QLD1',
    'AUS-SA':  'SA1',
    'AUS-TAS': 'TAS1',
    'AUS-VIC': 'VIC1',
}


generators = nem.stationdata().generators
print(generators)
nemImporter = nem.importer()
nemFile = nemImporter.SCADA[-1]
dispatch = nemFile.filter("UNIT_SCADA")
print(dispatch)


def fetch_price(country_code=None, session=None):
    """Requests the last known power price of a given country
    Arguments:
    country_code (optional) -- used in case a parser is able to fetch multiple countries
    session (optional)      -- request session passed in order to re-use an existing session
    Return:
    A dictionary in the form:
    {
      'countryCode': 'FR',
      'currency': EUR,
      'datetime': '2017-01-01T00:00:00Z',
      'price': 0.0,
      'source': 'mysource.com'
    }
    """

def fetch_exchange(country_code1=None, country_code2=None, session=None):
    """Requests the last known power exchange (in MW) between two countries
    Arguments:
    country_code (optional) -- used in case a parser is able to fetch multiple countries
    session (optional)      -- request session passed in order to re-use an existing session
    Return:
    A dictionary in the form:
    {
      'sortedCountryCodes': 'DK->NO',
      'datetime': '2017-01-01T00:00:00Z',
      'netFlow': 0.0,
      'source': 'mysource.com'
    }
    """

def fetch_production(country_code=None, session=None):
    """Requests the last known production mix (in MW) of a given country
    Arguments:
    country_code (optional) -- used in case a parser is able to fetch multiple countries
    session (optional)      -- request session passed in order to re-use an existing session
    Return:
    A dictionary in the form:
    {
      'countryCode': 'FR',
      'datetime': '2017-01-01T00:00:00Z',
      'production': {
          'biomass': 0.0,
          'coal': 0.0,
          'gas': 0.0,
          'hydro': 0.0,
          'nuclear': null,
          'oil': 0.0,
          'solar': 0.0,
          'wind': 0.0,
          'geothermal': 0.0,
          'unknown': 0.0
      },
      'storage': {
          'hydro': -10.0,
      },
      'source': 'mysource.com'
    }
    """


if __name__ == '__main__':
    """Main method, never used by the Electricity Map backend, but handy for testing."""

    print('fetch_production("AUS-NSW") ->')
    print(fetch_production('AUS-NSW'))
    print('fetch_production("AUS-QLD") ->')
    print(fetch_production('AUS-QLD'))
    print('fetch_production("AUS-SA") ->')
    print(fetch_production('AUS-SA'))
    print('fetch_production("AUS-TAS") ->')
    print(fetch_production('AUS-TAS'))
    print('fetch_production("AUS-VIC") ->')
    print(fetch_production('AUS-VIC'))
    print("fetch_exchange('AUS-NSW', 'AUS-QLD') ->")
    print(fetch_exchange('AUS-NSW', 'AUS-QLD'))
    print("fetch_exchange('AUS-NSW', 'AUS-VIC') ->")
    print(fetch_exchange('AUS-NSW', 'AUS-VIC'))
    print("fetch_exchange('AUS-VIC', 'AUS-SA') ->")
    print(fetch_exchange('AUS-VIC', 'AUS-SA'))
    print("fetch_exchange('AUS-VIC', 'AUS-TAS') ->")
    print(fetch_exchange('AUS-VIC', 'AUS-TAS'))