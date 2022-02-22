#!/bin/sh
export INPUT_AIRPORT="/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Apache_Beam/Airline_project/data/airports.csv"
export INPUT_FLIGHT="/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Apache_Beam/Airline_project/data/201501_part.csv"
export OUTPUT_BIGQUERY="mlopsmac:flights.simevents"
export OUTPUT_FLIGHT="/Users/macthibichngoc/Documents/Works/DATA_ENGINEER/Apache_Beam/Airline_project/data/"
export TEMP_LOCAITON="gs://dataflow-mac/flights/temp/"

python bat_pipeline/main.py --runner=DirectRunner \
    --project="mlopsmac" \
    --side_input="${INPUT_AIRPORT}" \
    --input="${INPUT_FLIGHT}" \
    --output="${OUTPUT_BIGQUERY}" \
    --output_flight="${OUTPUT_FLIGHT}" \
    --temp_location="${TEMP_LOCAITON}"

