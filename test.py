import nem

P5url = "http://www.nemweb.com.au/Reports/CURRENT/P5_Reports/"

nemImporter = nem.importer()
print(nemImporter.p5[0].filter("REGIONSOLUTION")[0])
print(nemImporter.CO2[0].filter("PUBLISHING")[0])
