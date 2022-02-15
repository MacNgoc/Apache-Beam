import logging
import unittest
import pytz
import os
import sys
import csv
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from pipelines.airport_pipeline import addtimezone, add_24h_if_before, as_utc, tz_correct

def create_timezone_airport_dict():
    timezone_dict = {}
    with open('../data/airports.csv', 'r') as csvfile:
        file_text = csv.reader(csvfile)
        for fields in file_text:
            timezone = addtimezone(fields[21], fields[26])
            timezone_dict[fields[0]]= timezone

    with open('../data/airport_dict.json', "w") as outfile:
        json.dump(timezone_dict, outfile)
    return timezone_dict

class TestDataIngestion(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)

    def test_addtimezone(self):
        """ test the function addtimezone """
        # lat, lon = (49.009724, 2.547778) ## aéroport de Charles de Gaulles 'Europe/Paris'
        # lat, lon = (48.69716100, 2.18961700) # Orsay 
        # lat, lon = (51.470020, -0.454295) # Heathrow UTC 'Europe/London'
        # lat, lon = (40.712784, -74.005941) # NEwyor UTC−5 'America/New_York'
        # lat, lon = (38.889248, -77.050636) # Washington Dc # UTC−5
        # lat, long = (19.741755,-155.844437) # Hawaï
        # 'America/New_York' (40.641766, -73.780968)
        paris_lat, paris_lon = (49.009724, 2.547778)
        newyork_lat, newyork_lon = (40.712784, -74.005941)
        no_data_lat, no_data_lon = ('', '')

        expected_output_paris = (49.009724, 2.547778,'Europe/Paris')
        expected_output_newyork =  (40.712784, -74.005941,'America/New_York')
        expected_output_no_data = ('', '', 'TIMEZONE')

       
        
        actual_output_paris = addtimezone(paris_lat, paris_lon)
        actual_output_newyork = addtimezone(newyork_lat, newyork_lon)
        actual_output_no_data = addtimezone(no_data_lat, no_data_lon)
           

        self.assertEqual(actual_output_paris, expected_output_paris)
        self.assertEqual(actual_output_newyork, expected_output_newyork)
        self.assertEqual(actual_output_no_data, expected_output_no_data)

    def test_as_utc(self):

        #valid input
        input = ('2015-01-01', '1259', "Australia/Melbourne")
        expected_ouput = ('2015-01-01T01:59:00', 39600.0)
        actual_output = as_utc(input[0], input[1], input[2])

        self.assertEqual(actual_output, expected_ouput)

        # not valid iput - raise error
        with self.assertRaises(ValueError):
            as_utc('', '1200', "Australia/Melbourne")

        #not valid input -return ('', 0)
        actual_output_invalid_hour = as_utc('2014-02-01', '', "Australia/Melbourne")
        expected_ouput_invalid_hour = ('', 0)
        self.assertEqual(actual_output_invalid_hour, expected_ouput_invalid_hour)
        
        # not valid timezone 
        with self.assertRaises(pytz.exceptions.UnknownTimeZoneError):
            as_utc('2014-02-01', '1200', " ")
       

    def test_add_24_if_before(self):
        arr_time = '2015-01-01T01:59:00'
        depart_time = '2015-01-01T20:59:00'

        #test valid input - change arr_time
        expected_output = '2015-01-02T01:59:00'
        actual_change_output = add_24h_if_before(arr_time, depart_time)
        self.assertEqual(actual_change_output, expected_output)

        # test valid input - no change arr_time
        actual_no_change_ouput = add_24h_if_before('2015-01-01T20:59:00', '2015-01-01T01:59:00')
        self.assertEqual(actual_no_change_ouput, '2015-01-01T20:59:00')

    def test_tz_correct(self):

        data = {'FL_DATE': '2015-01-01', 'UNIQUE_CARRIER': 'AA', 'AIRLINE_ID': '19805', 'CARRIER': 'AA', \
            'FL_NUM': '1', 'ORIGIN_AIRPORT_ID': '12478', 'ORIGIN_AIRPORT_SEQ_ID': '1247802',\
            'ORIGIN_CITY_MARKET_ID': '31703', 'ORIGIN': 'JFK', 'DEST_AIRPORT_ID': '12892', \
            'DEST_AIRPORT_SEQ_ID': '1289203', 'DEST_CITY_MARKET_ID': '32575', 'DEST': 'LAX', \
            'CRS_DEP_TIME': '0900', 'DEP_TIME': '0855', 'DEP_DELAY': '-5.00', 'TAXI_OUT': '17.00', \
            'WHEELS_OFF': '0912', 'WHEELS_ON': '1230', 'TAXI_IN': '7.00', 'CRS_ARR_TIME': '1230',\
            'ARR_TIME': '1237', 'ARR_DELAY': '7.00', 'CANCELLED': '0.00', 'CANCELLATION_CODE': '', \
            'DIVERTED': '0.00', 'DISTANCE': '2475.00'}

        airport_timezones_dict = {"1247802": ["51.72194444", "19.39805556", "Europe/London"]}
        expected_output = {'FL_DATE': '2015-01-01', 'UNIQUE_CARRIER': 'AA', 'AIRLINE_ID': '19805', \
            'CARRIER': 'AA', 'FL_NUM': '1', 'ORIGIN_AIRPORT_ID': '12478', 'ORIGIN_AIRPORT_SEQ_ID': '1247802',\
            'ORIGIN_CITY_MARKET_ID': '31703', 'ORIGIN': 'JFK', 'DEST_AIRPORT_ID': '12892', \
            'DEST_AIRPORT_SEQ_ID': '1289203', 'DEST_CITY_MARKET_ID': '32575', 'DEST': 'LAX', \
            'CRS_DEP_TIME': '2015-01-01T09:00:00', 'DEP_TIME': '2015-01-01T08:55:00', 'DEP_DELAY': '-5.00',\
            'TAXI_OUT': '17.00', 'WHEELS_OFF': '2015-01-01T09:12:00', 'WHEELS_ON': '2015-01-01T18:30:00', \
            'TAXI_IN': '7.00', 'CRS_ARR_TIME': '2015-01-01T18:30:00', 'ARR_TIME': '2015-01-01T18:37:00', \
            'ARR_DELAY': '7.00', 'CANCELLED': '0.00', 'CANCELLATION_CODE': '', 'DIVERTED': '0.00', \
            'DISTANCE': '2475.00', 'DEP_AIRPORT_LAT': '51.72194444', 'DEP_AIRPORT_LON': '19.39805556',\
            'ARR_AIRPORT_LON': '-92.17', 'DEP_AIRPORT_TZOFFSET': 0.0, 'ARR_AIRPORT_TZOFFSET': -21600.0}

        actual_ouput = next(tz_correct(data, airport_timezones_dict))
        
        self.assertEqual(actual_ouput, expected_output)


       
        


test = TestDataIngestion()
test.test_addtimezone()
test.test_as_utc()
test.test_add_24_if_before()
test.test_tz_correct()
        



