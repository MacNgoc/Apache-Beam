#!/bin/sh
export INPUT_AIRPORT="gs://dataflow-mac/flights/airports/airports.csv.gz"
export INPUT_FLIGHT="gs://dataflow-mac/flights/raw/*.csv"
export OUTPUT_BIGQUERY="mlopsmac:flights.simevents"
export OUTPUT_FLIGHT="gs://dataflow-mac/flights/tzcorr/all_flights"
export TEMP_LOCAITON="gs://dataflow-mac/flights/temp/"
export GCP_PROJECT="mlopsmac"
export REGION="us-central1"
export BUCKET="dataflow-mac"

python bat_pipeline/main.py --runner=DataflowRunner \
    --project="${GCP_PROJECT}" \
    --bucket="${BUCKET}" \
    --region="${REGION}" \
    --setup_file=./bat_pipeline/setup.py \
    --side_input="${INPUT_AIRPORT}" \
    --input="${INPUT_FLIGHT}" \
    --output="${OUTPUT_BIGQUERY}" \
    --output_flight="${OUTPUT_FLIGHT}" \
    --temp_location="${TEMP_LOCAITON}"

