import logging
import argparse
import json
import numpy as np
import os
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window  as window


#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Airflow/google_cloud/mlopsmac-080afb6fa874.json"


DATETIME_FORMAT='%Y-%m-%dT%H:%M:%S'

def add_airport(event):
    if event['EVENT_TYPE'] == 'departed':
        return event['ORIGIN'], event
    else:
        return event['DEST'], event

def compute_stats(airport, events):
    arrived = [float(event['ARR_DELAY']) for event in events if event['EVENT_TYPE'] == 'arrived' ]
    avg_arr_delay = float(np.mean(arrived)) if len(arrived) > 0 else None

    departed = [float(event['DEP_DELAY']) for event in events if event['EVENT_TYPE'] == 'departed']
    avg_dep_delay = float(np.mean(departed)) if len(departed) > 0 else None

    num_flights = len(events)
    start_time = min([datetime.strptime(event['EVENT_TIME'], DATETIME_FORMAT) for event in events])
    latest_time = max([datetime.strptime(event['EVENT_TIME'], DATETIME_FORMAT) for event in events])

    return {
        'AIRPORT' : airport,
        'AVG_ARR_DELAY': avg_arr_delay,
        'AVG_DEP_DELAY': avg_dep_delay,
        'NUM_FLIGHTS' : num_flights,
        'START_TIME': start_time,
        'END_TIME': latest_time
    }


def run(project, output_table, beam_args = None): 

    argv = [
        '--job_name=ch04avgdelay',
        '--streaming',
        '--save_main_session',
        '--staging_location=gs://dataflow-mac/flights/staging/',
        '--temp_location=gs://dataflow-mac/flights/temp/',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--max_num_workers=3',
        '--region=us-central1',
    ]

    beam_options = PipelineOptions(
        beam_args,
        job_name='ch04avgdelay',
        project='mlopsmac',
        temp_location="gs://dataflow-mac/flights/temp/",
        region='us-central1',
        streaming=True,
        save_main_session=True

    )
    
    
    with beam.Pipeline(options=beam_options) as pipeline :
        events = {}

        for event_name in ["arrived", "departed"]:
            topic_name = f"projects/{project}/topics/{event_name}"
            
            events[event_name] = (pipeline  
                | "read: {}".format(event_name)  >> beam.io.ReadFromPubSub(topic=topic_name).with_output_types(bytes)
                | "UTF-8 bytes to string {}".format(event_name)  >> beam.Map(lambda msg : msg.decode("utf-8") )
                | "parse:{}".format(event_name)  >> beam.Map(lambda msg : json.loads(msg) )
        )


        all_events = (events['arrived'], events['departed']) | beam.Flatten()

        stats = (all_events
                    | "add_airport"  >> beam.Map(add_airport) 
                    | "windowing "  >>   beam.WindowInto(window.SlidingWindows(60*60, 5*60))
                    | "group by key"  >> beam.GroupByKey()
                    | "computing statistic "  >> beam.Map(lambda x : compute_stats(x[0], x[1]))
        )

        stats_schema = ','.join(['AIRPORT:string,AVG_ARR_DELAY:float,AVG_DEP_DELAY:float',
                                 'NUM_FLIGHTS:int64,START_TIME:timestamp,END_TIME:timestamp'])
        (stats
            |   'bqout' >> beam.io.WriteToBigQuery(
                    'flights.streaming_delays', schema=stats_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )



if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)

    
    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p', '--project', help = 'Project Name', default = 'mlopsmac')
    parser.add_argument('-o', '--output_table', help = 'Output Bigquery table', default = 'mlopsmac:flights.streaming_delays')
    
    known_args, beam_args = parser.parse_known_args()

    print("known parameters", known_args)
    print("known beam parameters", beam_args)

    run(known_args.project, known_args.output_table, beam_args)
    
