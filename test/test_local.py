import apache_beam as beam
import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from apache_beam.options.pipeline_options import PipelineOptions
import csv
from pipelines.airport_pipeline import addtimezone, add_24h_if_before, as_utc, tz_correct, get_next_event, create_row
from pipelines.airport_pipeline import convert_csv_to_dict
from pipelines.schema_table_bigquery import table_schema

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Airflow/google_cloud/mlopsmac-080afb6fa874.json"




pipeline = beam.Pipeline()


airports = (pipeline 
        | "airports: read" >> beam.io.ReadFromText('../data/airports.csv.gz')
        | "airports: fields" >> beam.Map(lambda line: next(csv.reader([line])))
        | "airports:tz" >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        #| "print_out" >> beam.Map(print)
    )

flights = (pipeline
        | "flights:read" >> beam.io.ReadFromText('../data/201501_part.csv')
        | "flights:convert to dict" >> beam.FlatMap(convert_csv_to_dict )
        | 'flights: tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
    )

(flights 
        | "flights:tostring" >> beam.Map(lambda fields: json.dumps(fields) )
        | "flights:out" >> beam.io.WriteToText("../data/test_flight_out")
    )

events = flights | beam.FlatMap(get_next_event)


(events
        | 'events:totablerow' >> beam.Map(lambda fields: create_row(fields) )
        | 'events:out'  >> beam.io.WriteToBigQuery(
                                'mlopsmac:flights.simevents',
                                schema=table_schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                custom_gcs_temp_location = 'gs://dataflow-mac/data/flights/temp/' )
    )  

pipeline.run()