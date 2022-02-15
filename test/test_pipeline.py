import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import unittest
import logging
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam

from pipelines.airport_pipeline import addtimezone, add_24h_if_before, as_utc, tz_correct

#['1672801', '16728', 'N3C', 'Macon County', 'Franklin, NC', '3601', '36', 'United States', 'US', 
# 'North Carolina', 'NC', '37', '3670701', '36707', 'Franklin, NC', '3601', '36', '35', 'N', '13', 
# '20', '35.22222222', '83', 'W', '25', '12', '-83.42000000', '-0500', '2016-06-01', '', '0', '1', '']

class TransformationAirportTest(unittest.TestCase):

    def create_pipeline(self):
        return TestPipeline()

    def test_airport_transformation(self):
        
        data_test = ['1659101', '16591', 'HSG', 'Saga Airport', 'Saga, Japan', '73601', '736', 'Japan', 'JP', '', '', '', \
            '3657601', '36576', 'Saga, Japan', '73601', '736', '33', 'N', '8', '59', '33.14972222', '130', 'E', '18', '8', \
            '130.30222222', '+0900', '2013-07-01', '', '0', '1', '']
        with self.create_pipeline() as p:
            input = p | beam.Create(data_test)
            output = input | beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
            assert_that(
                output,
                equal_to([ ('1659101', ('33.14972222', '130.30222222', 'Asia/Tokyo')) ])
            )

    def test_flight_pipeline(self):

        data_test = ['1659101', '16591', 'HSG', 'Saga Airport', 'Saga, Japan', '73601', '736', 'Japan', 'JP', '', '', '', \
            '3657601', '36576', 'Saga, Japan', '73601', '736', '33', 'N', '8', '59', '33.14972222', '130', 'E', '18', '8', \
            '130.30222222', '+0900', '2013-07-01', '', '0', '1', '']
        
        pipeline = self.create_pipeline()

        side_input = pipeline | 'side' >> beam.Create(data_test)
        side_input = pipeline | beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        
        with self.create_pipeline() as p:
            input = p | beam.Create(data_test)
            output = input | beam.FlatMap(tz_correct, beam.pvalue.AsDict(side_input)) 
            assert_that(
                output,
                equal_to([ ('1659101', ('33.14972222', '130.30222222', 'Asia/Tokyo')) ])
            )

    def test_end_to_end_pipeline():
        pass


        

