"""
This is an example on how to use the nem module to read data from AEMO.

.. code-block:: python

    import nem

    for document in nem.current.DispatchIS:
    for data in document.filter("PRICE"):
        if data["REGIONID"] == "VIC1":
            print(data["RRP"])
            
    # prints out the RRP for VIC

    print(nem.generators()["GSTONE1"]["Fuel Source - Primary"])
    # 'Fossil'
    # prints out the primary fuel source for gstone
"""

from nem.importer import importer
from nem.importer import document
from nem.stationdata import stationdata

current = importer()
"""Provides a ``nem.importer`` for current data"""
archive = importer(url="http://www.nemweb.com.au/Reports/Archive/")
"""Provides a ``nem.importer`` for archive data"""
historical = importer(url="http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/", historical=True)
"""Provides a ``nem.importer`` for historical data (10+ years)"""
direct = document
"""Provides a ``nem.importer.document`` for single use file extraction"""


generators = stationdata
"""Dictonary of generators and their data from the partipants list"""