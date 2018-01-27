# The request library is used to fetch content through HTTP
import requests
from pyexcel_xls import get_data
from io import BytesIO

class stationdata(dict):
    def __init__(self, url="https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/Current-Participants/NEM-Registration-and-Exemption-List.xls"):
        """
        Provides generator information from the AEMO current list of registrations and exemptions list as AEMO does not publish this data as part of the normal dataset.

        :rtype: dictonary
        """
        # parse the xls document for NEM registration details
        stationdata = requests.get(url)
        f = BytesIO()
        f.write(stationdata.content)

        data = get_data(f)
        gen_table = data["Generators and Scheduled Loads"]
        gen_table_headers = gen_table[0]
        for row in gen_table[1:]:
            col_i = 0
            generator = {}
            for col in row:
                generator[gen_table_headers[col_i]] = col
                col_i += 1
            if "DUID" in generator:
                self[generator["DUID"]] = generator