from nem.importer import importer

current = importer()
archive = importer(url="http://www.nemweb.com.au/Reports/Archive/")
historical = importer(url="http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/", historical=True)

