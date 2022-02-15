
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from  pipelines.schema_table_bigquery import table_schema
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Airflow/google_cloud/mlopsmac-080afb6fa874.json"


DATETIME_FORMAT='%Y-%m-%dT%H:%M:%S'


def addtimezone(lat, lon):
    """
    compute timezones based on lat and lon
    input:
        lat: float
        lon: float
    output:
        lat: float
        lon: float
        timezone: str
    """
    try:
        import timezonefinder
        tf = timezonefinder.TimezoneFinder()
        lat = float(lat)
        lon = float(lon)
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
        #return (lat, lon, 'America/Los_Angeles') # FIXME
    except ValueError:
        return lat, lon, 'TIMEZONE' # header

def as_utc(date, hhmm, tzone):
    """
    convert local time to UTC time
    input:
        date: str '2015-01-20'
        hhmm: str '1240'
        tzone: str 'America/Los_Angeles'
    
    output:
        utc_time: '2015-02-20T20:40:00'
        utc_offset: -28800.0 (second)
    """
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False)
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
        else:
            return '',0 # empty string corresponds to canceled flights
    except ValueError as e:
        print('{} {} {}'.format(date, hhmm, tzone))
        raise e

def add_24h_if_before(arrtime, deptime):
    """
    add 24h if the date of departure before the date of arrival
    input:
        arrtime: '2015-01-01 08:00:00 UTC' or '2015-01-01T01:59:00'
        deptime: '2015-01-01 20:00:00 UTC'
    output:
        arrtime: '2015-01-02 08:00:00 UTC'
    """
    import datetime
    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime



def airport_timezone(airport_id, airport_timezones):
        if airport_id in airport_timezones:
            return airport_timezones[airport_id]
        else:
            return ('37.52', '-92.17', u'America/Chicago')


def tz_correct(fields, airport_timezones):
    """
    convert local time in a line of dataset to UTC time based on airport_timezones_dict
    input:
        fields: dict {'FL_DATE': }
        airport_timezones_dict: {"1330304": ["51.72194444", "19.39805556", "Europe/Warsaw"]}
    output:
        ['2015-01-01', 'AA', '19805', 'AA', '1605', '13303', '1330303', '32467', 'MIA', '11298', '1129803', '30194', 'DFW', '2015-01-01T19:35:00', '2015-01-01T19:33:00', '-2.00', '9.00', '2015-01-01T19:42:00', '2015-01-01T21:21:00', '11.00', '2015-01-01T21:47:00', '2015-01-01T21:32:00', '-15.00', '0.00', '', '0.00', '1121.00', '37.52', '-92.17', '-21600.0', '37.52', '-92.17', '-21600.0']

    """
    #fields['FL_DATE'] = fields['FL_DATE'].strftime('%Y-%m-%d')
    
    dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
    arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]

    fields["DEP_AIRPORT_LAT"], fields["DEP_AIRPORT_LON"], dep_timezone = airport_timezone(dep_airport_id, airport_timezones) 
    fields["ARR_AIRPORT_LON"], fields["ARR_AIRPORT_LON"], arr_timezone = airport_timezone(arr_airport_id, airport_timezones)
    

    for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]: 
        fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)

    for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]: 
        fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
      
    for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
        fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])

    fields["DEP_AIRPORT_TZOFFSET"] = deptz
    fields["ARR_AIRPORT_TZOFFSET"] = arrtz

    yield fields

def convert_csv_to_dict(line):
    header = "FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE".split(",")
    list_data = line.split(",")
    if "FL_DATE" not in list_data:
        dict_data = dict(zip(header, list_data))
        yield dict_data


def get_next_event(fields):
    """
    create event 'departed' and 'arrived'
    input:
        fields: list of str
    output:
        fields + event
    """
    if len(fields['DEP_TIME']) > 0:
        event = dict(fields)
        event["EVENT_TYPE"] = "departed"
        event["EVENT_TIME"] = fields["DEP_TIME"]
        for f in ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # not knowable at departure time
        yield event
    
    if len(fields["WHEELS_OFF"]) > 0:
        event = dict(fields)  # copy
        event["EVENT_TYPE"] = "wheelsoff"
        event["EVENT_TIME"] = fields["WHEELS_OFF"]
        for f in ["WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # not knowable at departure time
        yield event

    if len(fields["ARR_TIME"]) > 0:
        event = dict(fields)
        event["EVENT_TYPE"] = "arrived"
        event["EVENT_TIME"] = fields["ARR_TIME"]
        yield event


def create_row(fields):
    
    featdict = dict(fields)
    featdict['EVENT_DATA'] = json.dumps(fields)
    return featdict




def run():

    import argparse
    
    
    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p', '--project', help = 'Unique project ID', required=True)
    parser.add_argument('-b','--bucket', help='Bucket where your data were ingested', default='dataflow-mac')
    parser.add_argument('-r', '--region', help = 'Region in which to run the Dataflow job. Choose the same region as your bucket.', default='us-central1')
    parser.add_argument('-d', '--dataset', help = 'Bigquery dataset', default = 'flights')
    parser.add_argument('-ru', '--runner', help = "executed environment", default = 'DataflowRunner')

    
    known_args, beam_args = parser.parse_known_args()


    beam_options = PipelineOptions(
        beam_args,
        runner = known_args.runner,
        project = known_args.project,
        job_name = "timecorrection-1",
        temp_location='gs://{}/flights/staging/'.format(known_args.bucket),
        region = known_args.region,
        save_main_session=True
    )

    airports_filename = 'gs://{}/flights/airports/airports.csv.gz'.format(known_args.bucket)
    flights_raw_file = 'gs://{}/flights/raw/*.csv'.format(known_args.bucket)
    flights_output = 'gs://{}/flights/tzcorr/all_flights'.format(known_args.bucket)
    events_output = '{}:{}.simevents'.format(known_args.project, known_args.dataset)
    gcs_location='gs://{}/flights/temps/'.format(known_args.bucket)


    pipeline = beam.Pipeline(options=beam_options)

    print("Correcting timestamps and writing to Bigquery dataset {}".format(known_args.dataset))

    airports = (pipeline 
        | "airports: read" >> beam.io.ReadFromText(airports_filename)
        | "airports: fields" >> beam.Map(lambda line: next(csv.reader([line])))
        | "airports:tz" >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
    )

    flights = (pipeline
        | "flights:read" >> beam.io.ReadFromText(flights_raw_file)
        | "flights:convert to dict" >> beam.FlatMap(convert_csv_to_dict )
        | 'flights: tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
    )

    (flights 
        | "flights:tostring" >> beam.Map(lambda fields: json.dumps(fields) )
        | "flights:out" >> beam.io.WriteToText(flights_output)
    )

    events = flights | beam.FlatMap(get_next_event)


    (events
        | 'events:totablerow' >> beam.Map(lambda fields: create_row(fields) )
        | 'events:out'  >> beam.io.WriteToBigQuery(
                                events_output,
                                schema=table_schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                custom_gcs_temp_location = gcs_location )
    )  
    pipeline.run()

if __name__ == '__main__':

    #lat, lon = '33.14972222', '130.30222222'
    #print(addtimezone(lat, lon))


    # test as_utc
    input = ('2015-02-20', '1240', "America/Los_Angeles")
    actual_output = as_utc(input[0], input[1], input[2])
    print(actual_output)

    # # test add_24h_if_before
    # print(add_24h_if_before('2015-01-02 08:00:00 UTC', '2015-01-01 20:00:00'))
    # test = add_24h_if_before('2015-01-01T01:59:00', '2015-01-01T20:59:00')
    # print(test)


    # airport_timezones_dict = {"1247802": ["51.72194444", "19.39805556", "Europe/Paris"]}
    # data = {'FL_DATE': '2015-01-01', 'UNIQUE_CARRIER': 'AA', 'AIRLINE_ID': '19805', 'CARRIER': 'AA',\
    #         'FL_NUM': '1', 'ORIGIN_AIRPORT_ID': '12478', 'ORIGIN_AIRPORT_SEQ_ID': '1247802', \
    #         'ORIGIN_CITY_MARKET_ID': '31703', 'ORIGIN': 'JFK', 'DEST_AIRPORT_ID': '12892', \
    #         'DEST_AIRPORT_SEQ_ID': '1289203', 'DEST_CITY_MARKET_ID': '32575', 'DEST': 'LAX',\
    #         'CRS_DEP_TIME': '0900', 'DEP_TIME': '0855', 'DEP_DELAY': '-5.00', 'TAXI_OUT': '17.00', \
    #         'WHEELS_OFF': '0912', 'WHEELS_ON': '1230', 'TAXI_IN': '7.00', 'CRS_ARR_TIME': '1230', \
    #         'ARR_TIME': '1237', 'ARR_DELAY': '7.00', 'CANCELLED': '0.00', 'CANCELLATION_CODE': '', \
    #         'DIVERTED': '0.00', 'DISTANCE': '2475.00'}
    # resultat = tz_correct(data, airport_timezones_dict)
    # print(next(resultat))


